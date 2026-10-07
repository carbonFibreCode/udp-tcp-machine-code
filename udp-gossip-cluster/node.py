"""
Peer-to-peer cluster membership over UDP using gossip.

Each node starts knowing at most one other node (its seed). Nodes discover the
whole cluster by gossiping their peer lists, and drop peers that go silent.

  python3 node.py 9000            # first node, no seed
  python3 node.py 9001 9000       # joins through seed 9000
  python3 node.py 9002 9001       # joins through seed 9001

Commands (type into a running node):
  peer-list    show every live peer this node can gossip to

How it works:
  * JOIN: a new node sends JOIN to its seed; the seed replies with its full view
  * gossip: every second, send "PEERS <list>" to one random peer; receivers merge it
  * heartbeat: every second, send a small PING to every known peer
  * anyone who sends me a message is a live peer (last_seen = now)
  * failure detection: a peer not heard from for PEER_TIMEOUT seconds is removed,
    and stale gossip about it is ignored unless it contacts us directly again
"""
import random
import socket
import sys
import threading
import time

GOSSIP_INTERVAL = 1.0
PEER_TIMEOUT = 5.0

my_port = int(sys.argv[1])
seed = int(sys.argv[2]) if len(sys.argv) > 2 else None

peers = {}                     # port -> last time we heard from it (directly or via gossip)
dead = {}                      # port -> when we declared it dead (ignore gossip about it for a while)
lock = threading.Lock()
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind(("127.0.0.1", my_port))


def send(port, msg):
    sock.sendto(msg.encode(), ("127.0.0.1", port))


def my_view():
    """My peers + myself, as 'PEERS 9000,9001,9002'."""
    with lock:
        return "PEERS " + ",".join(str(p) for p in set(peers) | {my_port})


def touch(port):
    """Call with lock held: mark a peer as alive now."""
    if port != my_port:
        peers[port] = time.time()
        dead.pop(port, None)


def listener():
    while True:
        data, (_, sender) = sock.recvfrom(4096)
        msg = data.decode(errors="replace").strip()
        with lock:
            touch(sender)                                  # whoever talks to me is alive
        if msg == "JOIN":
            send(sender, my_view())                        # tell the newcomer everyone I know
        elif msg.startswith("PEERS "):
            with lock:
                now = time.time()
                for p in msg[6:].split(","):
                    if not p.isdigit():
                        continue
                    p = int(p)
                    recently_dead = now - dead.get(p, 0) < 3 * PEER_TIMEOUT
                    if p not in peers and not recently_dead:
                        touch(p)                           # learn new peers (stale gossip can't revive the dead)


def gossiper():
    while True:
        time.sleep(GOSSIP_INTERVAL)
        with lock:
            now = time.time()
            for p in [p for p, seen in peers.items() if now - seen > PEER_TIMEOUT]:
                del peers[p]
                dead[p] = now
                print(f"[-] peer {p} timed out")
            targets = list(peers)
        for p in targets:
            send(p, "PING")                                # cheap liveness signal to everyone
        if targets:
            send(random.choice(targets), my_view())        # share my view with one random peer


threading.Thread(target=listener, daemon=True).start()
threading.Thread(target=gossiper, daemon=True).start()
if seed:
    send(seed, "JOIN")
print(f"node {my_port} up" + (f", seed {seed}" if seed else " (first node)"))

for line in sys.stdin:
    if line.strip() == "peer-list":
        with lock:
            print("peers:", sorted(peers))
