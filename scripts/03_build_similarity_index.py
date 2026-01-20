#!/usr/bin/env python3
import json, os, sqlite3
from pathlib import Path

def main():
    try:
        import faiss  # type: ignore
    except Exception as e:
        raise SystemExit(
            "faiss is not installed.\n"
            "Try: pip install faiss-cpu\n"
            "If that fails on Mac: conda install -c conda-forge faiss-cpu\n"
            f"\nOriginal error: {e}"
        )
    try:
        from sentence_transformers import SentenceTransformer  # type: ignore
    except Exception as e:
        raise SystemExit(
            "sentence-transformers is not installed.\n"
            "Run: pip install sentence-transformers\n"
            f"\nOriginal error: {e}"
        )

    root = Path(__file__).resolve().parents[1]
    db_path = root / "data" / "processed" / "planning.db"
    out_index = root / "index" / "app_index.faiss"
    out_meta = root / "index" / "app_index_meta.json"

    if not db_path.exists():
        raise SystemExit(f"Missing DB: {db_path}")

    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(applications)")
    cols = [r[1] for r in cur.fetchall()]

    id_col = "application_number" if "application_number" in cols else ("id" if "id" in cols else cols[0])
    text_candidates = [c for c in ["description", "proposal", "development_description", "address", "site_address", "location"] if c in cols]
    if not text_candidates:
        raise SystemExit(f"Couldn't find text columns. Columns were: {cols}")

    select_cols = [id_col] + text_candidates
    cur.execute(f"SELECT {', '.join(select_cols)} FROM applications")
    rows = cur.fetchall()
    conn.close()

    texts, meta = [], []
    for row in rows:
        app_id = str(row[0])
        parts = [str(x) for x in row[1:] if x not in (None, "", "None")]
        text = " | ".join(parts).strip()
        if not text:
            continue
        texts.append(text)
        meta.append({"id": app_id, "text_fields": parts})

    if not texts:
        raise SystemExit("No usable text rows found in applications.")

    model_name = os.getenv("ST_MODEL", "all-MiniLM-L6-v2")
    model = SentenceTransformer(model_name)

    emb = model.encode(texts, show_progress_bar=True, convert_to_numpy=True, normalize_embeddings=True)
    dim = emb.shape[1]

    index = faiss.IndexFlatIP(dim)
    index.add(emb)

    faiss.write_index(index, str(out_index))
    out_meta.write_text(json.dumps({"model": model_name, "count": len(texts), "meta": meta}, indent=2), encoding="utf-8")

    print(f"OK: built index with {len(texts)} vectors")
    print(f"Wrote: {out_index}")
    print(f"Wrote: {out_meta}")

if __name__ == "__main__":
    main()
