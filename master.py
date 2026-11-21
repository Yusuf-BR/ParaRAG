"""The master: starts shards, spreads the corpus, scatters queries, gathers and merges answers."""
import multiprocessing as mp, queue, socket, threading, time
from net import send_msg, recv_msg
from shard import serve, doc_freq

class Cluster:
    def __init__(self, n_shards, base_port=5000, timeout=2.0):
        self.ports = [base_port + i for i in range(n_shards)]
        self.timeout = timeout
        self.procs = [self._start(p) for p in self.ports]
        self.parts, self.df, self.n = [], {}, 0

    def _start(self, port):
        proc = mp.Process(target=serve, args=(port,), daemon=True)
        proc.start()
        for _ in range(100):                                   # wait until the shard accepts connections
            try:
                socket.create_connection(("127.0.0.1", port), timeout=0.2).close()
                return proc
            except OSError:
                time.sleep(0.05)
        raise RuntimeError(f"shard on port {port} did not start")

    def _call(self, port, msg):
        with socket.create_connection(("127.0.0.1", port), timeout=self.timeout) as s:
            send_msg(s, msg)
            return recv_msg(s)

    def _scatter(self, msgs):
        """One thread per shard (waiting on sockets = I/O, so threads are enough). None = shard failed."""
        out = queue.Queue()
        def ask(port, msg):
            try:
                out.put((port, self._call(port, msg)))
            except (OSError, EOFError):
                out.put((port, None))
        threads = [threading.Thread(target=ask, args=(p, m)) for p, m in zip(self.ports, msgs)]
        for t in threads: t.start()
        for t in threads: t.join()
        return dict(out.get() for _ in threads)

    def load(self, chunks):
        self.parts = [chunks[i::len(self.ports)] for i in range(len(self.ports))]
        self.df, self.n = doc_freq(chunks), len(chunks)
        self._scatter([("load", part, self.df, self.n) for part in self.parts])

    def search(self, query, k=5):
        replies = self._scatter([("search", query, k)] * len(self.ports))
        hits = [h for r in replies.values() if r for h in r]
        dead = [p for p, r in replies.items() if r is None]
        return sorted(hits, reverse=True)[:k], dead            # global top-k is inside the union of shard top-k

    def kill(self, i):
        self.procs[i].terminate(); self.procs[i].join()

    def revive(self):
        for i, proc in enumerate(self.procs):
            if not proc.is_alive():
                self.procs[i] = self._start(self.ports[i])
                self._call(self.ports[i], ("load", self.parts[i], self.df, self.n))

    def close(self):
        for p in self.procs:
            p.terminate(); p.join()