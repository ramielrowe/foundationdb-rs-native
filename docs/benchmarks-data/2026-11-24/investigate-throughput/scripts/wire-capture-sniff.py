# Minimal packet capture for a sidecar container that shares a disposable FDB
# container's network namespace: writes Ethernet frames of TCP port 4500 traffic on eth0 to a
# pcap file until SIGTERM or the time limit. Usage: python sniff.py OUT.pcap SECONDS
import socket, struct, sys, time, signal
out, secs = sys.argv[1], float(sys.argv[2])
s = socket.socket(socket.AF_PACKET, socket.SOCK_RAW, socket.htons(0x0800))
s.bind(("eth0", 0))
s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 64 << 20)
s.settimeout(0.5)
f = open(out, "wb")
f.write(struct.pack("<IHHiIII", 0xa1b2c3d4, 2, 4, 0, 0, 262144, 1))
stop = False
def onsig(*a):
    global stop
    stop = True
signal.signal(signal.SIGTERM, onsig)
end = time.time() + secs
n = 0
while not stop and time.time() < end:
    try:
        pkt = s.recv(262144)
    except socket.timeout:
        continue
    if len(pkt) < 34 or pkt[23] != 6:
        continue
    ihl = (pkt[14] & 0x0f) * 4
    sp, dp = struct.unpack("!HH", pkt[14 + ihl:14 + ihl + 4])
    if sp != 4500 and dp != 4500:
        continue
    t = time.time()
    f.write(struct.pack("<IIII", int(t), int((t % 1) * 1e6), len(pkt), len(pkt)))
    f.write(pkt)
    n += 1
f.close()
print("captured", n, file=sys.stderr)
