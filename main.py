import logging
import socket
import queue
import time
from threading import Thread

import influxdb

# from influxdb_client import InfluxDBClient
# from influxdb_client.client.write_api import SYNCHRONOUS

UDP_IP = "0.0.0.0"
UDP_PORT = 8001


def get_logger(verbose=False):
    log_format = '%(asctime)s %(levelname)s [%(module)s] %(message)s'
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format=log_format, datefmt='%H:%M:%S')
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    return logger


logger = get_logger()

Q = queue.Queue(200_000)


def receive_thread():
    logger.info("receiving on %s:%s", UDP_IP, UDP_PORT)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((UDP_IP, UDP_PORT))

    while True:
        data, addr = sock.recvfrom(1024 * 2)
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
    client = influxdb.InfluxDBClient(
        "homeassistant.local",
        username="home_assistant",
        password="h0me",
    )

    Thread(target=receive_thread, daemon=True).start()


    while True:
        batch = []
        while not Q.empty() and len(batch) < 20_000:
            batch.append(Q.get())

        if len(batch) > 0:
            logger.info('Writing %d', len(batch))
            try:
                client.write(batch, protocol='line', params=dict(db='open_pe', precision='ms'))
            except Exception as e:
                logger.error('Error writing batch: %s', e)

        time.sleep(.1)


main()
