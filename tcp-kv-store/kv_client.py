"""
Client for kv_server.py. Type commands, see replies.  (nc localhost 9001 also works)
Run: python3 kv_client.py
"""
import socket

s = socket.create_connection(("127.0.0.1", 9001))
f = s.makefile("rw", encoding="utf-8", newline="\n")
while True:
    cmd = input("kv> ")
    if cmd.strip().upper() == "QUIT":
        break
    f.write(cmd + "\n")
    f.flush()
    reply = f.readline()
    if not reply:                       # "" = server closed the connection
        print("server disconnected")
        break
    print(reply.rstrip())
s.close()
