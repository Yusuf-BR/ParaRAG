"""Last step of RAG: a Groq-hosted LLM writes the answer from the retrieved passages."""
import os
from dotenv import load_dotenv

load_dotenv()                                         # reads GROQ_API_KEY from the .env file

MODEL = os.getenv("GROQ_MODEL", "openai/gpt-oss-120b")

def generate(question, hits):
    if not os.getenv("GROQ_API_KEY"):
        return None                                   # no key: the CLI just shows the passages
    from groq import Groq
    context = "\n\n".join(f"[{f} p.{p}] {t}" for _, f, p, t in hits)
    prompt = ("Answer the question using ONLY the excerpts below. Cite sources like [file p.N]. "
              "If the excerpts do not contain the answer, say so. Answer in the language of the question.\n\n"
              f"{context}\n\nQuestion: {question}")
    reply = Groq().chat.completions.create(model=MODEL, messages=[{"role": "user", "content": prompt}])
    return reply.choices[0].message.content