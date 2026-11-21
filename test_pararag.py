import glob, os, random, tempfile, unittest
from ingest import ingest
from make_corpus import make_corpus
from master import Cluster
from shard import Index, doc_freq

def fake_chunks(n=400):
    rng = random.Random(1)
    vocab = [f"w{i}" for i in range(150)]
    return [(f"f{i % 7}.pdf", i % 5 + 1, " ".join(rng.choices(vocab, k=60))) for i in range(n)]

QUERIES = ["w1 w2 w3", "w10 w99", "w42", "w7 w8 w9 w150"]

class ClusterTests(unittest.TestCase):
    port = 5100
    def setUp(self):
        ClusterTests.port += 10
        self.chunks = fake_chunks()
        self.ref = Index(self.chunks, doc_freq(self.chunks), len(self.chunks))
        self.cluster = Cluster(4, base_port=ClusterTests.port, timeout=1.0)
        self.cluster.load(self.chunks)
    def tearDown(self):
        self.cluster.close()

    def test_distributed_equals_single_index(self):
        for q in QUERIES:
            hits, dead = self.cluster.search(q, 5)
            self.assertEqual(dead, [])
            self.assertEqual(hits, self.ref.search(q, 5))

    def test_dead_shard_degrades_without_crashing(self):
        self.cluster.kill(1)
        hits, dead = self.cluster.search("w1 w2 w3", 5)
        self.assertEqual(dead, [self.cluster.ports[1]])
        self.assertTrue(hits)
        self.assertTrue(set(hits) <= set(self.ref.search("w1 w2 w3", 400)))

    def test_revive_restores_full_answers(self):
        self.cluster.kill(2)
        self.cluster.revive()
        for q in QUERIES:
            hits, dead = self.cluster.search(q, 5)
            self.assertEqual(dead, [])
            self.assertEqual(hits, self.ref.search(q, 5))

class IngestTests(unittest.TestCase):
    def test_parallel_equals_serial_and_finds_the_planted_fact(self):
        with tempfile.TemporaryDirectory() as d:
            make_corpus(d, n_docs=6, pages=3)
            paths = sorted(glob.glob(os.path.join(d, "*.pdf")))
            serial = ingest(paths, 1, "serial")
            self.assertEqual(serial, ingest(paths, 3, "processes"))
            idx = Index(serial, doc_freq(serial), len(serial))
            _, f, p, _ = idx.search("zorblax protocol leader", 1)[0]
            self.assertEqual((f, p), ("doc003.pdf", 3))

    def test_damaged_pdfs(self):
        with tempfile.TemporaryDirectory() as d:
            make_corpus(d, n_docs=4, pages=2)
            with open(os.path.join(d, "doc000.pdf"), "rb") as f:
                good = f.read()
            with open(os.path.join(d, "truncated.pdf"), "wb") as f:
                f.write(good[: int(len(good) * 0.8)])
            with open(os.path.join(d, "garbage.pdf"), "wb") as f:
                f.write(b"%PDF-1.7 not a real file")
            paths = sorted(glob.glob(os.path.join(d, "*.pdf")))
            for mode in ("serial", "processes"):
                files = {c[0] for c in ingest(paths, 2, mode)}
                self.assertIn("truncated.pdf", files)        # repaired by MuPDF
                self.assertNotIn("garbage.pdf", files)       # skipped, not fatal

if __name__ == "__main__":
    unittest.main()