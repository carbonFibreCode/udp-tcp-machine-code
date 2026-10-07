"""
Multi-group TCP chat server.

  * users join with a unique user id and a group id (groups are created on demand)
  * every message is broadcast to all members of the sender's group
  * new members receive the group's recent history
  * history is persisted to disk (JSON lines) and survives restarts
  * only the last 15 minutes of messages are kept; the log is compacted periodically

Client = telnet / nc:
  telnet localhost 9002
  > JOIN <user_id> <group_id>      (must be first)
  > hello everyone                 (anything else = a message to your group)
  > /switch <group_id>             (leave this group, join another)
  > /who                           (members of your group)
  > /quit

Run:  python3 chat_server.py 9002
"""
import json
import os
import socket
import sys
import threading
import time
from collections import defaultdict, deque

HISTORY_FILE = "chat_history.jsonl"
RETENTION_SECONDS = 15 * 60


class ChatState:
    def __init__(self):
        self.lock = threading.Lock()
        self.groups = defaultdict(dict)        # group_id -> {user_id: Client}
        self.history = defaultdict(deque)      # group_id -> deque[(ts, user, text)]
        self.users = set()                     # user ids currently online (must be unique)
        self._load()
        self._file = open(HISTORY_FILE, "a", encoding="utf-8")

    # ---- persistence ----
    def _load(self):
        if not os.path.exists(HISTORY_FILE):
            return
        cutoff = time.time() - RETENTION_SECONDS
        with open(HISTORY_FILE, encoding="utf-8") as f:
            for line in f:
                try:
                    m = json.loads(line)
                except json.JSONDecodeError:
                    continue                    # half-written last line after a crash
                if m["ts"] >= cutoff:
                    self.history[m["group"]].append((m["ts"], m["user"], m["text"]))

    def record(self, group, user, text):
        """Caller holds self.lock."""
        ts = time.time()
        self.history[group].append((ts, user, text))
        self._file.write(json.dumps({"ts": ts, "group": group, "user": user, "text": text}) + "\n")
        self._file.flush()
        return ts

    # ---- retention ----
    def recent(self, group):
        """Caller holds self.lock. Drops messages older than 15 min, returns the rest."""
        cutoff = time.time() - RETENTION_SECONDS
        q = self.history[group]
        while q and q[0][0] < cutoff:
            q.popleft()
        return list(q)

    def compact_forever(self, interval=60):
        """Rewrite the file with only recent messages so it doesn't grow forever."""
        while True:
            time.sleep(interval)
            with self.lock:
                cutoff = time.time() - RETENTION_SECONDS
                tmp = HISTORY_FILE + ".tmp"
                with open(tmp, "w", encoding="utf-8") as f:
                    for g, q in self.history.items():
                        while q and q[0][0] < cutoff:
                            q.popleft()
                        for ts, u, t in q:
                            f.write(json.dumps({"ts": ts, "group": g, "user": u, "text": t}) + "\n")
                self._file.close()
                os.replace(tmp, HISTORY_FILE)          # atomic rename
                self._file = open(HISTORY_FILE, "a", encoding="utf-8")


class Client:
    def __init__(self, conn, addr):
        self.conn, self.addr = conn, addr
        self.file = conn.makefile("rw", encoding="utf-8", newline="\n")
        self.send_lock = threading.Lock()      # two threads may write to one socket
        self.user = None
        self.group = None

    def send(self, text):
        try:
            with self.send_lock:
                self.file.write(text + "\n")
                self.file.flush()
        except OSError:
            pass                               # dead client; its own thread cleans it up


def fmt(ts, user, text):
    return f"[{time.strftime('%H:%M:%S', time.localtime(ts))}] {user}: {text}"


def broadcast(state, group, text, exclude=None):
    """Caller holds state.lock."""
    for uid, c in list(state.groups[group].items()):
        if c is not exclude:
            c.send(text)


def join(state, c, group):
    with state.lock:
        state.groups[group][c.user] = c
        c.group = group
        history = state.recent(group)
        broadcast(state, group, f"*** {c.user} joined {group}", exclude=c)
        # send history while still holding the lock, so no live message can
        # arrive before the history it follows
        c.send(f"*** joined group {group} ({len(history)} messages in last 15 min)")
        for ts, u, t in history:              # history for new members
            c.send(fmt(ts, u, t))


def leave(state, c):
    with state.lock:
        if c.group and c.user in state.groups[c.group]:
            del state.groups[c.group][c.user]
            broadcast(state, c.group, f"*** {c.user} left {c.group}")
            if not state.groups[c.group]:
                del state.groups[c.group]
        c.group = None


def serve(state, conn, addr):
    c = Client(conn, addr)
    c.send("Welcome. First line: JOIN <user_id> <group_id>")
    try:
        for line in c.file:
            line = line.strip()
            if not line:
                continue

            if c.user is None:                               # handshake
                parts = line.split()
                if len(parts) != 3 or parts[0].upper() != "JOIN":
                    c.send("ERR first line must be: JOIN <user_id> <group_id>")
                    continue
                with state.lock:
                    if parts[1] in state.users:
                        c.send("ERR user id already online, pick another")
                        continue
                    state.users.add(parts[1])
                c.user = parts[1]
                join(state, c, parts[2])
                continue

            if line == "/quit":
                break
            if line == "/who":
                with state.lock:
                    c.send("*** members: " + ", ".join(state.groups[c.group]))
                continue
            if line.startswith("/switch "):
                new_group = line.split(maxsplit=1)[1].strip()
                leave(state, c)
                join(state, c, new_group)
                continue

            with state.lock:                                 # relay to the group
                ts = state.record(c.group, c.user, line)
                broadcast(state, c.group, fmt(ts, c.user, line))
    except (ConnectionResetError, BrokenPipeError, UnicodeDecodeError):
        pass
    finally:
        leave(state, c)
        with state.lock:
            state.users.discard(c.user)
        conn.close()
        print(f"[-] {addr} {c.user}")


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9002
    state = ChatState()
    threading.Thread(target=state.compact_forever, daemon=True).start()
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("0.0.0.0", port))
    srv.listen(128)
    print(f"chat server on :{port}")
    try:
        while True:
            conn, addr = srv.accept()
            print(f"[+] {addr}")
            threading.Thread(target=serve, args=(state, conn, addr), daemon=True).start()
    except KeyboardInterrupt:
        print("\nbye")
    finally:
        srv.close()


if __name__ == "__main__":
    main()
