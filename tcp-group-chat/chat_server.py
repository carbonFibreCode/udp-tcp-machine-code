"""
Group chat server (Media.net/Directi SRE task). Lean version: objectives 1-3.

  1. clients connect with telnet/nc
  2. JOIN <user_id> <group_id> -> every message goes to everyone in that group
  3. a new member first receives the group's previous messages

Run:   python3 chat_server.py
Test:  nc localhost 9002   (two or three terminals)
       > JOIN alice g1
       > hello

Only TALK about (don't build): saving history to a file so a restart keeps it,
keeping only the last 15 minutes (store a timestamp with each message and drop old ones).
"""
import socket
import threading

groups = {}                 # group_id -> {user_id: file object to write to}
history = {}                # group_id -> ["alice: hi", ...]
lock = threading.Lock()


def send(f, text):
    try:
        f.write(text + "\n")
        f.flush()
    except OSError:
        pass                # that client died; its own thread cleans it up


def broadcast(group, text):
    """Call with lock held."""
    for f in groups[group].values():
        send(f, text)


def handle_client(conn, addr):
    f = conn.makefile("rw", encoding="utf-8", newline="\n")
    user = group = None
    send(f, "Welcome! Type: JOIN <user_id> <group_id>")
    try:
        for line in f:
            line = line.strip()
            if not line:
                continue

            if user is None:                                # not joined yet
                parts = line.split()
                if len(parts) != 3 or parts[0].upper() != "JOIN":
                    send(f, "ERR first type: JOIN <user_id> <group_id>")
                    continue
                with lock:
                    if any(parts[1] in members for members in groups.values()):
                        send(f, "ERR user id already taken")
                        continue
                    user, group = parts[1], parts[2]
                    groups.setdefault(group, {})            # new group auto-created
                    history.setdefault(group, [])
                    for old in history[group]:              # objective 3: history
                        send(f, old)
                    groups[group][user] = f
                    broadcast(group, f"*** {user} joined {group}")
                continue

            if line == "/quit":
                break

            msg = f"{user}: {line}"                         # objective 2: relay to group
            with lock:
                history[group].append(msg)
                broadcast(group, msg)
    except (ConnectionResetError, BrokenPipeError, UnicodeDecodeError):
        pass
    finally:
        if user:
            with lock:
                del groups[group][user]
                broadcast(group, f"*** {user} left")
        conn.close()


def main():
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("0.0.0.0", 9002))
    srv.listen()
    print("chat server on :9002")
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
