import socket

UDP_IP = "homeassistant.local"
UDP_PORT = 8086

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.sendto(bytes("hello world", 'utf-8'), (UDP_IP, UDP_PORT))
