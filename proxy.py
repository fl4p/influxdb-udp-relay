import queue
import socket
import sys
import time
import zlib
from threading import Thread
from typing import List

import influxdb

import ha
from ha import read_user_options
from util import get_logger

logger = get_logger()



class Proxy:

    def __init__(self, listen_port, target_host, target_port):
        self.listen_port = listen_port
        self.target_host = target_host
        self.target_port = target_port

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(('0.0.0.0', self.listen_port))
        s.listen()
        print('listening on port {}...'.format(self.listen_port))
        self.s = s

    def start(self):
        self.thread = Thread(target=self._thread, daemon=True)
        self.thread.start()
        logger.info('started proxy :%s -> %s:%s', self.listen_port, self.target_host, self.target_port)

    def _thread(self):

        connections = []

        self.s.setblocking(False)
        while True:

            try:
                conn, addr = self.s.accept()
            except BlockingIOError:
                conn, addr = None, None

            if conn:
                print(f"Connected by {addr} (%d active" % len(connections))
                conn.setblocking(False)
                up = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                up.connect((self.target_host, self.target_port))
                up.setblocking(False)
                connections.append((conn, up))

            for conn, up in connections:
                try:
                    try:
                        data = conn.recv(1024)
                        if len(data) == 0:
                            raise ConnectionResetError()
                        up.sendall(data)
                    except BlockingIOError:
                        pass

                    try:
                        data = up.recv(1024)
                        if len(data) == 0:
                            raise ConnectionResetError()
                        conn.sendall(data)
                    except BlockingIOError:
                        pass
                except (ConnectionResetError, OSError):
                    print('closing connection', conn, up)
                    conn.close()
                    up.close()
                    connections.remove((conn, up))
                    break



            time.sleep(.01)


if __name__ == '__main__':
    listen_port = 8081
    target_host = "127.0.0.1"
    p = Proxy(9001, 'localhost', 9000 )
    p.start()
    time.sleep(9999)

