"""PDF -> chunks, in parallel. CPU-bound work, so processes.
(No threads here: PyMuPDF does not support being called from several threads.)"""
import os
from multiprocessing import Pool
import pymupdf

pymupdf.TOOLS.mupdf_display_errors(False)     # silence repair warnings on damaged files

SIZE, STEP = 100, 80          # chunk = 100 words, sliding by 80 (20-word overlap)

def extract(path):
    chunks = []
    try:
        for page_no, page in enumerate(pymupdf.open(path), 1):
            text = page.get_text()
            if text.count(". .") > 20:                # table of contents / list of figures: skip
                continue
            words = text.split()
            for i in range(0, len(words), STEP):
                chunks.append((os.path.basename(path), page_no, " ".join(words[i:i + SIZE])))
    except Exception as e:                            # a file that cannot be repaired must not stop the corpus
        print(f"!! skipped {os.path.basename(path)}: {e}")
        return []
    return chunks


def ingest(paths, workers=4, mode="processes"):
    if mode == "serial":
        results = map(extract, paths)
    else:
        with Pool(workers) as pool:
            results = pool.map(extract, paths)
    return [c for r in results for c in r]