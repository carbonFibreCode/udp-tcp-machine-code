"""
KV store server (Media.net SRE 2022 task). Lean version: what to build in the round.

  PUT <key> <value> [EX <seconds>]  -> OK
  GET <key>                         -> VALUE <value> | NOT_FOUND
  DELETE <key>                      -> DELETED | NOT_FOUND
  anything else                     -> ERR ...

Run:   python3 kv_server.py
Test:  nc localhost 9001

Only TALK about (don't build): saving to disk (append-only log like Redis AOF),
a background thread that cleans expired keys, sharding across servers.
"""
import socket
import threading
import time

store = {}                  # key -> value
expiry = {}                 # key -> time when it dies (only keys with EX)
lock = threading.Lock()     # all client threads share store/expiry


def is_alive(key):
    """Call with lock held. Deletes the key if it has expired (lazy expiry)."""
    if key in expiry and expiry[key] <= time.time():
        store.pop(key, None)
        expiry.pop(key, None)
    return key in store


def handle(line):
    parts = line.strip().split()
    if not parts:
        return "ERR empty command"
    cmd = parts[0].upper()

    if cmd == "PUT":
        ttl = None
        if len(parts) == 5 and parts[3].upper() == "EX":    # PUT key value EX 10
            try:
                ttl = float(parts[4])
            except ValueError:
                return "ERR ttl must be a number"
        elif len(parts) != 3:
            return "ERR usage: PUT <key> <value> [EX <seconds>]"
        key, value = parts[1], parts[2]
        with lock:
            store[key] = value
            if ttl:
                expiry[key] = time.time() + ttl
            else:
                expiry.pop(key, None)       # plain PUT removes an old TTL
        return "OK"

    if cmd == "GET":
        if len(parts) != 2:
            return "ERR usage: GET <key>"
        with lock:
            if is_alive(parts[1]):
                return "VALUE " + store[parts[1]]
        return "NOT_FOUND"

    if cmd == "DELETE":
        if len(parts) != 2:
            return "ERR usage: DELETE <key>"
        with lock:
            if is_alive(parts[1]):
                del store[parts[1]]
                expiry.pop(parts[1], None)
                return "DELETED"
        return "NOT_FOUND"

    return "ERR unknown command " + cmd


def handle_client(conn, addr):
    print(f"[+] {addr}")
    with conn:
        f = conn.makefile("rw", encoding="utf-8", newline="\n")
        try:
            for line in f:
                f.write(handle(line) + "\n")
                f.flush()
        except (ConnectionResetError, BrokenPipeError):
            pass                            # client vanished; keep serving others
    print(f"[-] {addr}")


def main():
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("0.0.0.0", 9001))
    srv.listen()
    print("kv server on :9001")
    try:
        while True:
            conn, addr = srv.accept()
            threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()
    except KeyboardInterrupt:
        print("\nbye")
    finally:
        srv.close()


if __name__ == "__main__":
    main()
