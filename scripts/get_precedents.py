#!/usr/bin/env python3
import os, json, argparse
from pathlib import Path

import numpy as np
import faiss
from sentence_transformers import SentenceTransformer

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("query", help="Proposal text")
    ap.add_argument("-k", type=int, default=5, help="Top K results")
    args = ap.parse_args()

    root = Path(__file__).resolve().parents[1]

    faiss_path = Path(os.getenv(
        "FAISS_INDEX_PATH",
        root / "index" / "app_index.faiss"
    ))
    meta_path = Path(os.getenv(
        "FAISS_META_PATH",
        root / "index" / "app_index_meta.json"
    ))

    if not faiss_path.exists() or not meta_path.exists():
        print("[]")
        return

    index = faiss.read_index(str(faiss_path))
    meta = json.loads(meta_path.read_text(encoding="utf-8"))

    # YOUR schema: {"model":..., "count":..., "meta":[...]}
    items = meta.get("meta", [])
    if not isinstance(items, list):
        items = []

    model = SentenceTransformer("all-MiniLM-L6-v2")
    emb = model.encode([args.query], normalize_embeddings=True)
    emb = np.asarray(emb, dtype="float32")

    scores, ids = index.search(emb, args.k)

    out = []
    for rank in range(len(ids[0])):
        idx = int(ids[0][rank])
        if idx < 0 or idx >= len(items):
            continue
        out.append({
            "rank": rank + 1,
            "score": float(scores[0][rank]),
            "meta": items[idx],
        })

    print(json.dumps(out, ensure_ascii=False))

if __name__ == "__main__":
    main()
