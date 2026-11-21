"""A shard = a small TCP server that owns a slice of the corpus and searches it."""
import heapq, math, re, socket
from collections import Counter
from net import send_msg, recv_msg

def tokens(text):
    return re.findall(r"\w+", text.lower())

def doc_freq(chunks):
    """In how many chunks does each word appear? Computed globally so scores are comparable across shards."""
    df = Counter()
    for _, _, text in chunks:
        df.update(set(tokens(text)))
    return df

class Index:
    """TF-IDF index over (file, page, text) chunks."""
    def __init__(self, chunks, df, n):
        self.idf = {w: math.log(1 + n / c) for w, c in df.items()}
        self.chunks = []
        for f, p, t in chunks:
            words = tokens(t)
            self.chunks.append((f, p, t, Counter(words), math.sqrt(len(words)) or 1))

    def search(self, query, k=5):
        q = [w for w in set(tokens(query)) if w in self.idf]
        scored = []
        for f, p, t, tf, norm in self.chunks:
            s = sum((1 + math.log(tf[w])) * self.idf[w] for w in q if w in tf)
            if s:
                scored.append((s / norm, f, p, t))
        return heapq.nlargest(k, scored)

def serve(port):
    index = None
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", port))            # local only: we unpickle what we receive
    srv.listen(5)
    while True:
        conn, _ = srv.accept()
        with conn:
            try:
                cmd, *args = recv_msg(conn)
                if cmd == "load":
                    index = Index(*args)     # args = (chunks, df, n)
                    send_msg(conn, "ok")
                elif cmd == "search":
                    send_msg(conn, index.search(*args) if index else [])
            except (OSError, EOFError):      # a client that connects and leaves must not kill the shard
                pass