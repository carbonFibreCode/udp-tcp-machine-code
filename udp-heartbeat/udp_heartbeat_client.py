"""
Heartbeat client: send a heartbeat, wait for the reply, sleep 1 s, repeat.

Key points to say to the SRE:
  * timeout on recv -> UDP can lose packets, never wait forever
  * sequence number -> know WHICH heartbeat the reply belongs to
  * 3 misses in a row -> mark server DOWN (that's a health check)

Run:  python3 udp_heartbeat_client.py     (kill the server to see DOWN)
"""
import socket
import time

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.settimeout(1.0)
seq = 0
misses = 0

while True:
    seq += 1
    start = time.time()
    sock.sendto(f"HEARTBEAT {seq}".encode(), ("127.0.0.1", 9003))
    try:
        data, _ = sock.recvfrom(1024)
        if data.decode() == f"HEARTBEAT {seq}":
            rtt = (time.time() - start) * 1000
            print(f"seq={seq} alive, rtt={rtt:.2f} ms")
            misses = 0
    except (socket.timeout, ConnectionRefusedError):     # no reply in 1 s / server not running
        misses += 1
        print(f"seq={seq} no reply")
        if misses == 3:
            print("*** server is DOWN")
    time.sleep(1)
