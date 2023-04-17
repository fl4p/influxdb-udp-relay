import socket
import queue
import time
from threading import Thread

import influxdb

import ha
from ha import read_user_options
from util import get_logger

UDP_IP = "0.0.0.0"

logger = get_logger()

Q = queue.Queue(200_000)
max_msg_len = 0

opt = read_user_options()


def receive_loop():
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
            Q.put(l, block=False)

        # logger.info("msg (%dB, %dL, Q=%d) from %s: %s", len(data), len(lines), Q.qsize(), addr[0], msg[:60])


def main():
    # client = InfluxDBClient(
    #    "http://homeassistant.local:8086",
    #               username="home_assistant",
    #               password="h0me",
    #               org="" # influxdb v1 had no org
    #               )
    # write = client.write_api(write_options=SYNCHRONOUS)

    config = ha.read_hass_configuration_yaml()
    influxdb_conf = config.get('influxdb')
    host = influxdb_conf.get('host', "homeassistant.local")
    user = influxdb_conf.get('username')

    logger.info('Connecting InfluxDB %s@%s', user, host)

    client = influxdb.InfluxDBClient(
        host=host,
        port=int(influxdb_conf.get('port', 8086)),
        # username="home_assistant", password="h0me",
        # username="hass", password="caravana",
        username=user,
        password=influxdb_conf.get('password'),
        database=influxdb_conf.get('database'),
    )

    logger.info('Measurements: %s', ','.join(map(lambda m:m.get('name', m),client.get_list_measurements())))

    rx_thread = Thread(target=receive_loop, daemon=True)
    rx_thread.start()

    while True:
        batch = []
        while not Q.empty() and len(batch) < 20_000:
            batch.append(Q.get())

        if len(batch) > 0:
            logger.info('Writing %d (max msg len=%d)', len(batch), max_msg_len)
            try:
                client.write(batch, protocol='line', params=dict(db='open_pe', precision='ms'))
            except Exception as e:
                logger.error('Error writing batch: %s', e)

        time.sleep(.2)


main()
