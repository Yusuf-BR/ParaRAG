"""Tiny message layer on top of TCP: a 4-byte length header + a pickled object."""
import pickle, struct

def send_msg(sock, obj):
    data = pickle.dumps(obj)
    sock.sendall(struct.pack("!I", len(data)) + data)   # sendall = the "loop until everything is sent" from the course

def recv_exact(sock, n):
    buf = b""
    while len(buf) < n:                                  # recv may return less than asked: loop (course, 4.3.h)
        part = sock.recv(n - len(buf))
        if not part:
            raise ConnectionError("connection closed")
        buf += part
    return buf

def recv_msg(sock):
    (size,) = struct.unpack("!I", recv_exact(sock, 4))
    return pickle.loads(recv_exact(sock, size))