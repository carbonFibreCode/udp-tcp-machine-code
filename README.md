# udp-tcp-machine-code

Networked programs built from scratch on raw TCP and UDP sockets in Python. No frameworks, no third-party libraries.

| Project | Transport | What it does |
|---|---|---|
| [`tcp-echo-server`](tcp-echo-server) | TCP | Multi-client echo server, one thread per client |
| [`tcp-kv-store`](tcp-kv-store) | TCP | Key-value store: GET / PUT / DELETE with key expiry (TTL), thread-safe |
| [`tcp-group-chat`](tcp-group-chat) | TCP | Group chat: messages go to everyone in your group; new members get the history |
| [`udp-heartbeat`](udp-heartbeat) | UDP | Echo server + heartbeat client that detects when the server goes down |
| [`udp-gossip-cluster`](udp-gossip-cluster) | UDP | Peer cluster: nodes join through a seed and discover each other by gossip |

Requirements: Python 3, and `nc` for testing.

## tcp-echo-server
```bash
python3 tcp-echo-server/echo_server.py
nc localhost 9000
```

## tcp-kv-store
```bash
python3 tcp-kv-store/kv_server.py
python3 tcp-kv-store/kv_client.py        # or: nc localhost 9001
kv> PUT name arun EX 10
kv> GET name
kv> DELETE name
```

## tcp-group-chat
```bash
python3 tcp-group-chat/chat_server.py
nc localhost 9002      # > JOIN alice g1   > hello
nc localhost 9002      # > JOIN bob g1     (sees alice's message)
```

## udp-heartbeat
```bash
python3 udp-heartbeat/udp_server.py
python3 udp-heartbeat/udp_heartbeat_client.py     # stop the server to see DOWN
```

## udp-gossip-cluster
```bash
python3 udp-gossip-cluster/node.py 9000           # first node, no seed
python3 udp-gossip-cluster/node.py 9001 9000      # joins via 9000
python3 udp-gossip-cluster/node.py 9002 9001      # joins via 9001
# type: peer-list
```
