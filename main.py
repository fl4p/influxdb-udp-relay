import queue
import socket
import sys
import time
from threading import Thread
from typing import List

import influxdb

import ha
from ha import read_user_options
from util import get_logger

UDP_IP = "0.0.0.0"

logger = get_logger()

import urllib3.exceptions

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class InfluxDBWriter():

    def _write_thread(self):
        while True:
            batch = []
            while not self.Q.empty() and len(batch) < 20_000:
                batch.append(self.Q.get())

            if len(batch) > 0:
                logger.info('Writing %d (max msg len=%d)', len(batch), max_msg_len)
                try:
                    self.client.write(batch, protocol='line', params=dict(db=self.client._database, precision='ms'))
                except Exception as e:
                    logger.error('Error writing batch: %s', e)

            time.sleep(.2)

    def __init__(self, influxdb_conf):
        self.host = influxdb_conf.get('host', "homeassistant.local")
        self.port = int(influxdb_conf.get('port', 8086))
        self.user = influxdb_conf.get('username')
        self.ssl = influxdb_conf.get('ssl', False)
        self.db = influxdb_conf.get('database')
        self.influxdb_conf = influxdb_conf

        self.key = f'{self.host}:{self.port}/{self.db}'

    def connect(self):

        logger.info('Connecting InfluxDB %s@%s (port=%i ssl=%s)', self.user, self.host, self.port, self.ssl)

        self.client = influxdb.InfluxDBClient(
            host=self.host,
            port=self.port,
            username=self.user,
            password=self.influxdb_conf.get('password'),
            database=self.db,
            ssl=self.ssl,
            verify_ssl=False,
            # org="" # influxdb v1 had no org
        )

        logger.info('Measurements: %s', ','.join(map(lambda m: m.get('name', m), self.client.get_list_measurements())))

        self.Q = queue.Queue(200_000)

        self.w_thread = Thread(target=self._write_thread, daemon=True)
        self.w_thread.start()


max_msg_len = 0

opt = read_user_options()


def receive_loop(writers: List[InfluxDBWriter]):
    global max_msg_len

    udp_port = int(opt.get('udp_port', 8086))

    logger.info("receiving on %s:%s", UDP_IP, udp_port)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((UDP_IP, udp_port))

    while True:
        data, addr = sock.recvfrom(1024 * 2)
        if len(data) > max_msg_len:
            max_msg_len = len(data)

        msg = data.decode("utf-8").rstrip('\n')
        lines = msg.split('\n')
        for l in lines:
            for w in writers:
                w.Q.put(l, block=False)

        # logger.info("msg (%dB, %dL, Q=%d) from %s: %s", len(data), len(lines), Q.qsize(), addr[0], msg[:60])


def main():
    # client = InfluxDBClient(
    #    "http://homeassistant.local:8086",
    #               username="home_assistant",
    #               password="",
    #               org="" # influxdb v1 had no org
    #               )
    # write = client.write_api(write_options=SYNCHRONOUS)

    try:
        config = ha.read_hass_configuration_yaml()
        influxdb_conf = config.get('influxdb', None)
    except Exception as e:
        logger.warning('Failed to load configuration: %s', str(e) or type(e))
        influxdb_conf = None

    writers = {}

    if influxdb_conf:
        w = InfluxDBWriter(influxdb_conf)
        writers[w.key] = w

    for influxdb_conf in opt.get('additional_servers', []):
        w = InfluxDBWriter(influxdb_conf)
        writers[w.key] = w

    if len(writers) == 0:
        logger.error('No servers!')
        sys.exit(1)

    for w in writers.values():
        w.connect()

    receive_loop(list(writers.values()))


main()
