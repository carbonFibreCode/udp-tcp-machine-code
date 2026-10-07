"""
UDP echo server that also answers heartbeats (PING n -> PONG n).

UDP is connectionless: no listen()/accept(). recvfrom() returns the sender's
address, sendto() replies to it.

Run:   python3 udp_server.py 9003 [drop_rate]
       drop_rate (0..1) randomly ignores packets to simulate packet loss.
Test:  nc -u localhost 9003
"""
import random
import socket
import sys


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9003
    drop = float(sys.argv[2]) if len(sys.argv) > 2 else 0.0
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)       # DGRAM = UDP
    sock.bind(("0.0.0.0", port))
    print(f"udp echo server on :{port} (simulated drop rate {drop:.0%})")
    while True:
        data, addr = sock.recvfrom(65535)                         # max UDP payload
        if random.random() < drop:
            print(f"dropped {data!r} from {addr}")
            continue
        msg = data.decode(errors="replace").strip()
        reply = f"PONG {msg[5:]}" if msg.startswith("PING ") else msg
        sock.sendto(reply.encode(), addr)


if __name__ == "__main__":
    main()
