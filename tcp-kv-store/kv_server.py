"""
Concurrent in-memory key-value store over TCP (a tiny Redis-like server).

Protocol (one command per line, one reply per line):
  PUT <key> <value...> [EX <seconds>]   -> OK
  GET <key>                             -> VALUE <value> | NOT_FOUND
  DELETE <key>                          -> DELETED | NOT_FOUND
  TTL <key>                             -> <seconds left> | -1 (no ttl) | NOT_FOUND
  KEYS                                  -> KEYS k1 k2 ...
  QUIT                                  -> BYE (server closes connection)
  anything else                         -> ERR <reason>

Design:
  * thread per client, shared store guarded by a threading.Lock
  * key expiry: lazy expiry on read + a background sweeper thread
  * durability: append-only log (AOF) replayed on startup, like Redis AOF

Run:   python3 kv_server.py 9001
Test:  python3 kv_client.py 9001     or    nc localhost 9001
"""
import os
import socket
import sys
import threading
import time

AOF_PATH = "kv.aof"


class KVStore:
    def __init__(self, aof_path=AOF_PATH):
        self._data = {}            # key -> value
        self._expiry = {}          # key -> absolute unix time it dies
        self._lock = threading.Lock()
        self._aof_path = aof_path
        self._replay()
        self._aof = open(aof_path, "a", encoding="utf-8")

    # ---------- persistence ----------
    def _replay(self):
        """Rebuild state from the log on startup."""
        if not os.path.exists(self._aof_path):
            return
        with open(self._aof_path, encoding="utf-8") as f:
            for line in f:
                parts = line.rstrip("\n").split(" ", 3)
                if parts[0] == "SET" and len(parts) == 4:
                    _, key, exp, value = parts
                    exp = float(exp)
                    if exp and exp <= time.time():
                        self._data.pop(key, None)
                        self._expiry.pop(key, None)
                        continue
                    self._data[key] = value
                    if exp:
                        self._expiry[key] = exp
                    else:
                        self._expiry.pop(key, None)
                elif parts[0] == "DEL" and len(parts) >= 2:
                    self._data.pop(parts[1], None)
                    self._expiry.pop(parts[1], None)

    def _log(self, line):
        self._aof.write(line + "\n")
        self._aof.flush()          # os.fsync() too if you want durability vs. power loss

    # ---------- helpers (call with lock held) ----------
    def _alive(self, key):
        exp = self._expiry.get(key)
        if exp is not None and exp <= time.time():
            self._data.pop(key, None)
            self._expiry.pop(key, None)
            return False
        return key in self._data

    # ---------- public API ----------
    def put(self, key, value, ttl=None):
        with self._lock:
            self._data[key] = value
            exp = time.time() + ttl if ttl else 0
            if ttl:
                self._expiry[key] = exp
            else:
                self._expiry.pop(key, None)
            self._log(f"SET {key} {exp} {value}")

    def get(self, key):
        with self._lock:
            return self._data[key] if self._alive(key) else None

    def delete(self, key):
        with self._lock:
            if not self._alive(key):
                return False
            del self._data[key]
            self._expiry.pop(key, None)
            self._log(f"DEL {key}")
            return True

    def ttl(self, key):
        with self._lock:
            if not self._alive(key):
                return None
            exp = self._expiry.get(key)
            return -1 if exp is None else int(exp - time.time() + 0.999)

    def keys(self):
        with self._lock:
            return [k for k in list(self._data) if self._alive(k)]

    def sweep_forever(self, interval=1.0):
        """Active expiry: free memory for keys nobody reads again."""
        while True:
            time.sleep(interval)
            with self._lock:
                now = time.time()
                for k in [k for k, e in self._expiry.items() if e <= now]:
                    self._data.pop(k, None)
                    self._expiry.pop(k, None)


def handle(line, store):
    parts = line.strip().split()
    if not parts:
        return "ERR empty command"
    cmd = parts[0].upper()

    if cmd == "PUT":
        if len(parts) < 3:
            return "ERR usage: PUT <key> <value> [EX <seconds>]"
        ttl = None
        if len(parts) >= 5 and parts[-2].upper() == "EX":
            try:
                ttl = float(parts[-1])
            except ValueError:
                return "ERR ttl must be a number"
            if ttl <= 0:
                return "ERR ttl must be > 0"
            parts = parts[:-2]
        store.put(parts[1], " ".join(parts[2:]), ttl)
        return "OK"
    if cmd == "GET":
        if len(parts) != 2:
            return "ERR usage: GET <key>"
        v = store.get(parts[1])
        return "NOT_FOUND" if v is None else f"VALUE {v}"
    if cmd in ("DELETE", "DEL"):
        if len(parts) != 2:
            return "ERR usage: DELETE <key>"
        return "DELETED" if store.delete(parts[1]) else "NOT_FOUND"
    if cmd == "TTL":
        if len(parts) != 2:
            return "ERR usage: TTL <key>"
        t = store.ttl(parts[1])
        return "NOT_FOUND" if t is None else str(t)
    if cmd == "KEYS":
        return "KEYS " + " ".join(store.keys())
    return f"ERR unknown command {cmd}"


def client_thread(conn, addr, store):
    print(f"[+] {addr}")
    with conn:
        f = conn.makefile("rw", encoding="utf-8", newline="\n")
        try:
            for line in f:
                if line.strip().upper() == "QUIT":
                    f.write("BYE\n")
                    f.flush()
                    break
                f.write(handle(line, store) + "\n")
                f.flush()
        except (ConnectionResetError, BrokenPipeError):
            pass                            # client vanished; don't crash the server
    print(f"[-] {addr}")


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9001
    store = KVStore()
    threading.Thread(target=store.sweep_forever, daemon=True).start()

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("0.0.0.0", port))
    srv.listen(128)
    print(f"kv server on :{port} (log: {AOF_PATH})")
    try:
        while True:
            conn, addr = srv.accept()
            threading.Thread(target=client_thread, args=(conn, addr, store), daemon=True).start()
    except KeyboardInterrupt:
        print("\nbye")
    finally:
        srv.close()


if __name__ == "__main__":
    main()
