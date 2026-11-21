"""python bench.py -> 1) why processes (GIL demo)  2) ingestion speedup  3) search latency vs shards."""
import glob, os, shutil, time
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool
from ingest import ingest
from make_corpus import make_corpus
from master import Cluster

N_DOCS, PAGES = 120, 100      # heavy enough that the work dwarfs the process start-up cost

def burn(n):                                  # pure-Python CPU work, like Calcul() in the course
    x = 0
    while x < n:
        x += 1

def build_corpus(folder, n_docs, pages):       # 4 distinct PDFs + copies: same parsing cost, much faster to generate
    make_corpus(folder, n_docs=4, pages=pages)
    originals = sorted(glob.glob(os.path.join(folder, "doc*.pdf")))
    for i in range(4, n_docs):
        shutil.copy(originals[i % 4], os.path.join(folder, f"copy{i:03d}.pdf"))

def timed(fn, *args):
    t = time.perf_counter()
    result = fn(*args)
    return result, time.perf_counter() - t

def best_of(k, fn, *args):                     # run k times, keep the fastest (removes cold-start noise)
    return min((timed(fn, *args) for _ in range(k)), key=lambda r: r[1])

def gil_demo(kind, jobs=4, n=20_000_000):
    if kind == "serial":
        for _ in range(jobs): burn(n)
    elif kind == "threads":
        with ThreadPoolExecutor(jobs) as ex: list(ex.map(burn, [n] * jobs))
    else:
        with Pool(jobs) as pool: pool.map(burn, [n] * jobs)

if __name__ == "__main__":
    cpus = os.cpu_count()

    print("\n1) CPU-BOUND JOBS: 4 x pure-Python loop (the GIL, as in the course)")
    base = None
    for kind in ("serial", "threads", "processes"):
        _, dt = timed(gil_demo, kind)
        base = base or dt
        print(f"  {kind:<10} {dt:6.2f}s   speedup x{base / dt:.2f}")

    folder = f"bench_corpus_{N_DOCS}x{PAGES}"
    if len(glob.glob(os.path.join(folder, "*.pdf"))) < N_DOCS:
        print(f"\ngenerating {N_DOCS} PDFs x {PAGES} pages (once)..."); build_corpus(folder, N_DOCS, PAGES)
    paths = sorted(glob.glob(os.path.join(folder, "*.pdf")))

    print(f"\n2) PDF INGESTION ({len(paths)} PDFs, {cpus} cores)")
    chunks, base = best_of(3, ingest, paths, 1, "serial")
    print(f"  serial     workers=1   {base:6.2f}s   speedup x1.00   efficiency 1.00")
    for w in sorted({2, 4, 8, cpus}):
        chunks, dt = best_of(3, ingest, paths, w, "processes")
        print(f"  processes  workers={w:<3}{dt:6.2f}s   speedup x{base / dt:.2f}   efficiency {base / dt / w:.2f}")

    big = chunks[:12000] * 5                               # 60k chunks (as before), so that search time is measurable
    query = " ".join(big[0][2].split()[:6])                # words that really exist in the corpus
    print(f"\n3) SEARCH ({len(big)} chunks, 30 queries)")
    base = None
    for n in (1, 2, 4, 8):
        cluster = Cluster(n, base_port=5500)
        cluster.load(big)
        _, dt = best_of(3, lambda: [cluster.search(query) for _ in range(30)])
        cluster.close()
        base = base or dt
        print(f"  shards={n}  {dt / 30 * 1000:7.1f} ms/query   speedup x{base / dt:.2f}")