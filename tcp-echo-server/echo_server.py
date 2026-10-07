"""
Step 1 of EVERY Media.net machine-coding task: a multi-client TCP echo server.
Everything else (KV store, chat server) is this file + a protocol on top.

Run:   python3 echo_server.py
Test:  telnet localhost 9000      (or: nc localhost 9000)
"""
import socket
import threading


def handle_client(conn: socket.socket, addr):
    print(f"[+] connected {addr}")
    with conn:
        # makefile() gives us readline(), so we don't have to split TCP bytes ourselves.
        # TCP is a byte STREAM: one send() != one recv(). Lines solve message framing.
        f = conn.makefile("rw", encoding="utf-8", newline="\n")
        for line in f:                      # loop ends when the client disconnects
            f.write("ECHO: " + line)
            f.flush()
    print(f"[-] disconnected {addr}")


def main():
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)          # IPv4 + TCP
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)         # restart without "Address already in use"
    srv.bind(("0.0.0.0", 9000))
    srv.listen()
    print("echo server listening on :9000")
    try:
        while True:
            conn, addr = srv.accept()                                 # blocks until a client connects
            threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()
    except KeyboardInterrupt:
        print("\nshutting down")
    finally:
        srv.close()


if __name__ == "__main__":
    main()
