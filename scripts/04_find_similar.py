import os, json, argparse
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer
import faiss
import numpy as np

load_dotenv()
INDEX_PATH = os.getenv("FAISS_INDEX_PATH", "./index/app_index.faiss")
META_PATH  = os.getenv("FAISS_META_PATH", "./index/meta.jsonl")

def load_meta(path):
    meta = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            meta.append(json.loads(line))
    return meta

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--text", required=True, help="Describe new case: address + proposal (+ notes)")
    ap.add_argument("--k", type=int, default=10)
    args = ap.parse_args()

    index = faiss.read_index(INDEX_PATH)
    meta = load_meta(META_PATH)

    model = SentenceTransformer("all-MiniLM-L6-v2")
    q = model.encode([args.text], normalize_embeddings=True).astype("float32")

    scores, idx = index.search(q, args.k)
    for rank in range(args.k):
        i = int(idx[0][rank])
        s = float(scores[0][rank])
        if i < 0 or i >= len(meta):
            continue
        print(f"{rank+1:02d}. score={s:.3f}  ref={meta[i]['application_ref']}  url={meta[i].get('url')}")
if __name__ == "__main__":
    main()
