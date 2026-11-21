"""Synthetic PDFs for tests and benchmarks (one planted fact in doc003.pdf, page 3)."""
import os, random
from fpdf import FPDF

def make_corpus(folder, n_docs=8, pages=5, seed=0):
    rng = random.Random(seed)
    vocab = ["".join(rng.choices("abcdefghijklmnopqrstuvwxyz", k=rng.randint(3, 9))) for _ in range(3000)]
    os.makedirs(folder, exist_ok=True)
    for d in range(n_docs):
        pdf = FPDF()
        for p in range(pages):
            pdf.add_page(); pdf.set_font("Helvetica", size=9)
            text = " ".join(rng.choices(vocab, k=350))
            if d == 3 and p == 2:
                text += " The zorblax protocol elects a leader using seventeen migrating islands."
            pdf.multi_cell(0, 4, text)
        pdf.output(os.path.join(folder, f"doc{d:03d}.pdf"))