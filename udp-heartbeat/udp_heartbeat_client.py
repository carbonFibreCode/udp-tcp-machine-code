"""
UDP heartbeat client / health checker.

Sends a heartbeat every second and waits for the reply:
  * timeout on every receive (UDP can silently lose packets)
  * sequence numbers to match replies to requests and ignore late ones
  * RTT and packet-loss statistics
  * marks the server DOWN after 3 consecutive missed heartbeats, and UP again on recovery

Run:  python3 udp_heartbeat_client.py [port] [host] [count]
"""
import socket
import sys
import time

TIMEOUT_S = 1.0
INTERVAL_S = 1.0
DOWN_AFTER_MISSES = 3


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9003
    host = sys.argv[2] if len(sys.argv) > 2 else "127.0.0.1"
    count = int(sys.argv[3]) if len(sys.argv) > 3 else 0          # 0 = forever

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(TIMEOUT_S)
    sent = received = consecutive_misses = 0
    rtts = []
    seq = 0
    try:
        while count == 0 or seq < count:
            seq += 1
            start = time.monotonic()                              # monotonic: immune to clock changes
            sock.sendto(f"PING {seq}".encode(), (host, port))
            sent += 1
            got = False
            while True:
                remaining = TIMEOUT_S - (time.monotonic() - start)
                if remaining <= 0:
                    break
                sock.settimeout(remaining)
                try:
                    data, _ = sock.recvfrom(1024)
                except socket.timeout:
                    break
                except ConnectionRefusedError:                     # ICMP port unreachable (server not running)
                    break
                if data.decode(errors="replace").strip() == f"PONG {seq}":
                    got = True
                    break                                          # else: stale reply to an old seq, keep waiting

            if got:
                rtt = (time.monotonic() - start) * 1000
                rtts.append(rtt)
                received += 1
                if consecutive_misses >= DOWN_AFTER_MISSES:
                    print("*** server is back UP")
                consecutive_misses = 0
                print(f"seq={seq} rtt={rtt:.2f} ms")
            else:
                consecutive_misses += 1
                print(f"seq={seq} TIMEOUT")
                if consecutive_misses == DOWN_AFTER_MISSES:
                    print(f"*** server DOWN ({DOWN_AFTER_MISSES} heartbeats missed)")
            time.sleep(INTERVAL_S)
    except KeyboardInterrupt:
        pass
    finally:
        loss = 100 * (sent - received) / sent if sent else 0
        print(f"\n{sent} sent, {received} received, {loss:.1f}% loss", end="")
        if rtts:
            print(f", rtt min/avg/max = {min(rtts):.2f}/{sum(rtts)/len(rtts):.2f}/{max(rtts):.2f} ms")
        else:
            print()


if __name__ == "__main__":
    main()
