# udp-tcp-machine-code

Five networked systems built from scratch on raw TCP and UDP sockets in Python, with no frameworks and no third-party libraries. Each one is a small, runnable version of a pattern used in real distributed infrastructure: a Redis-like key-value store, a group chat server, a health checker and a gossip-based cluster.

| Project | Transport | What it shows |
|---|---|---|
| [`tcp-echo-server`](tcp-echo-server) | TCP | Concurrent server skeleton: thread per client, newline message framing |
| [`tcp-kv-store`](tcp-kv-store) | TCP | Redis-like store: GET/PUT/DELETE, key expiry (TTL), append-only-log persistence, thread-safe state |
| [`tcp-group-chat`](tcp-group-chat) | TCP | Multi-group chat: broadcast to group members, history for new members, persistence, 15-minute retention |
| [`udp-heartbeat`](udp-heartbeat) | UDP | Health checker: timeouts, sequence numbers, RTT and packet-loss stats, DOWN/UP detection |
| [`udp-gossip-cluster`](udp-gossip-cluster) | UDP | Peer-to-peer membership: seed-based join, gossip propagation, failure detection |

Requirements: Python 3.8+, and `nc` or `telnet` for manual testing. Nothing to install.

---

## tcp-echo-server

The base every TCP server here is built on: `socket → bind → listen → accept`, with one thread per client. TCP is a byte stream, so messages are framed by newlines.

```bash
python3 tcp-echo-server/echo_server.py 9000
nc localhost 9000          # open several terminals; each line is echoed back
```

## tcp-kv-store

An in-memory key-value server that many clients can use at once.

```
PUT <key> <value> [EX <seconds>]   GET <key>   DELETE <key>   TTL <key>   KEYS   QUIT
```

- **Concurrency:** all clients share one store, guarded by a mutex.
- **Expiry:** keys with `EX` expire **lazily** (checked on access) and **actively** (a background sweeper thread frees keys nobody reads again). This is the same approach Redis uses.
- **Durability:** every write is appended to `kv.aof` and replayed on startup, so data survives a restart.

```bash
cd tcp-kv-store
python3 kv_server.py 9001
python3 kv_client.py 9001
kv> PUT session abc EX 10
OK
kv> GET session
VALUE abc
```

## tcp-group-chat

```bash
python3 tcp-group-chat/chat_server.py 9002
nc localhost 9002   →  JOIN alice g1  →  hello
nc localhost 9002   →  JOIN bob g1    (receives alice's message as history)
```

- User IDs are unique; groups are created when someone first joins them.
- Messages go to every member of the sender's group, and never to other groups.
- History is stored in a JSON-lines file, restored on restart, limited to the last 15 minutes, and compacted with an atomic file rename.
- A per-connection send lock prevents interleaved writes when several threads broadcast at once.

## udp-heartbeat

```bash
python3 udp-heartbeat/udp_server.py 9003 0.3        # 30% simulated packet loss
python3 udp-heartbeat/udp_heartbeat_client.py 9003  # kill the server to see DOWN
```

UDP can silently drop packets, so the client uses a receive timeout and sequence numbers to match replies, reports RTT and loss percentage, and marks the server **DOWN** after 3 consecutive misses (and **UP** on recovery). This is the same mechanism load-balancer health checks and liveness probes use.

## udp-gossip-cluster

```bash
cd udp-gossip-cluster
python3 node.py 9000            # first node, no seed
python3 node.py 9001 9000       # joins through 9000
python3 node.py 9002 9001       # joins through 9001 only
# type: peer-list   →   every node lists all the others
```

- **Join:** a new node sends `JOIN` to its seed and gets back the seed's whole view.
- **Gossip:** every second, each node sends its peer list to one random peer, which merges it. Knowledge spreads in about O(log N) rounds with no central coordinator.
- **Failure detection:** nodes send a small `PING` to all peers. A peer that is silent for 5 s is removed, and stale gossip can't bring it back unless it contacts the node directly again.
- UDP fits well here: a lost gossip message doesn't matter, because the next round repeats the information.

---

## Concepts covered

TCP vs UDP · message framing over byte streams · thread-per-client concurrency · mutexes and race conditions · lazy and active expiry · append-only-log persistence · atomic file replacement · timeouts and sequence numbers · failure detection · gossip protocols and eventual consistency
