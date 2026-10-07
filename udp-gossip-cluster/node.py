"""
UDP peer cluster with gossip.
  python3 node.py 9000            # first node, no seed
  python3 node.py 9001 9000       # joins via seed 9000
  python3 node.py 9002 9001       # joins via seed 9001
Type 'peer-list' to see every peer this node can gossip to.
"""
import random
import socket
import sys
import threading
import time

my_port = int(sys.argv[1])
seed = int(sys.argv[2]) if len(sys.argv) > 2 else None

peers = set()                  # ports of other nodes I know
lock = threading.Lock()
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind(("127.0.0.1", my_port))


def send(port, msg):
    sock.sendto(msg.encode(), ("127.0.0.1", port))


def my_view():
    """My peers + myself, as 'PEERS 9000,9001,9002'."""
    with lock:
        return "PEERS " + ",".join(str(p) for p in peers | {my_port})


def merge(ports):
    with lock:
        for p in ports:
            if p != my_port:
                peers.add(p)


def listener():
    while True:
        data, (_, sender) = sock.recvfrom(4096)
        msg = data.decode().strip()
        merge([sender])                         # whoever talks to me is a peer
        if msg == "JOIN":
            send(sender, my_view())             # tell the newcomer everyone I know
        elif msg.startswith("PEERS "):
            merge(int(p) for p in msg[6:].split(",") if p)


def gossiper():
    while True:
        time.sleep(1)
        with lock:
            targets = list(peers)
        if targets:
            send(random.choice(targets), my_view())   # share my list with one random peer


threading.Thread(target=listener, daemon=True).start()
threading.Thread(target=gossiper, daemon=True).start()
if seed:
    send(seed, "JOIN")
print(f"node {my_port} up" + (f", seed {seed}" if seed else " (first node)"))

while True:
    cmd = input().strip()
    if cmd == "peer-list":
        with lock:
            print("peers:", sorted(peers))
