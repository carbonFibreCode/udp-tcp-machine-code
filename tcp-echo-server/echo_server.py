"""
Multi-client TCP echo server.

One thread per client; newline-delimited messages (TCP is a byte stream,
so lines are used for message framing).

Run:   python3 echo_server.py 9000
Test:  nc localhost 9000      (open several terminals)
"""
import socket
import sys
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
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9000
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)          # IPv4 + TCP
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)         # restart without "Address already in use"
    srv.bind(("0.0.0.0", port))
    srv.listen()
    print(f"echo server listening on :{port}")
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
