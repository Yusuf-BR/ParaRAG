"""python main.py <folder_with_pdfs> [n_workers]"""
import glob, os, sys, time
from ingest import ingest
from master import Cluster
from generate import generate

if __name__ == "__main__":
    folder = sys.argv[1]
    n = int(sys.argv[2]) if len(sys.argv) > 2 else 4
    paths = sorted(glob.glob(os.path.join(folder, "*.pdf")))

    t = time.perf_counter()
    chunks = ingest(paths, n)
    print(f"{len(paths)} PDFs -> {len(chunks)} chunks in {time.perf_counter() - t:.2f}s ({n} processes)")
    if not chunks:
        sys.exit("No text could be extracted from the PDFs in that folder.")

    cluster = Cluster(n)
    cluster.load(chunks)
    print(f"{n} shards ready. Ask a question, or: /kill <i>  /revive  /quit")

    while True:
        line = input("question> ").strip()
        if line == "/quit":
            break
        elif line.startswith("/kill"):
            cluster.kill(int(line.split()[1])); print("shard killed")
        elif line == "/revive":
            cluster.revive(); print("dead shards restarted and reloaded")
        elif line:
            hits, dead = cluster.search(line)
            if not hits:
                print("  no matching passage found"); continue
            if dead:
                print(f"!! degraded mode: {len(dead)} shard(s) unreachable, part of the corpus is missing")
            for score, f, p, text in hits:
                print(f"  {score:.2f}  {f} p.{p}  {text[:90]}...")
            answer = generate(line, hits)
            if answer:
                print("\n" + answer)
    cluster.close()