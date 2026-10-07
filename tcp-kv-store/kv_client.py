"""
Interactive client for kv_server.py.

Run:  python3 kv_client.py [port] [host]
Then type commands like:  PUT name arun EX 10 / GET name / DELETE name / QUIT
"""
import socket
import sys


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9001
    host = sys.argv[2] if len(sys.argv) > 2 else "127.0.0.1"
    with socket.create_connection((host, port), timeout=5) as s:
        f = s.makefile("rw", encoding="utf-8", newline="\n")
        print(f"connected to {host}:{port}. Commands: PUT/GET/DELETE/TTL/KEYS/QUIT")
        while True:
            try:
                cmd = input("kv> ")
            except EOFError:
                cmd = "QUIT"
            if not cmd.strip():
                continue
            f.write(cmd + "\n")
            f.flush()
            reply = f.readline()
            if not reply:
                print("server closed the connection")
                break
            print(reply.rstrip())
            if cmd.strip().upper() == "QUIT":
                break


if __name__ == "__main__":
    main()
