"""
UDP echo server. UDP has no connection: no listen()/accept().
recvfrom() gives you the data AND who sent it; sendto() replies to them.

Run:   python3 udp_server.py
Test:  nc -u localhost 9003
"""
import socket

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)      # DGRAM = UDP
sock.bind(("0.0.0.0", 9003))
print("udp echo server on :9003")
while True:
    data, addr = sock.recvfrom(1024)
    sock.sendto(data, addr)                                  # echo it back
