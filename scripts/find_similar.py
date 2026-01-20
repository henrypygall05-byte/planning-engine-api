import sqlite3
import argparse
import numpy as np
from sentence_transformers import SentenceTransformer

DB_PATH = "data/processed/planning.db"
COUNCIL = "Newcastle City Council"

def from_blob(b: bytes, dim: int) -> np.ndarray:
    v = np.frombuffer(b, dtype=np.float32)
    if dim and v.shape[0] != dim:
        v = v[:dim]
    return v

def cosine_sim_matrix(query_vec: np.ndarray, mat: np.ndarray) -> np.ndarray:
    # If vectors are normalized, cosine = dot product
    return mat @ query_vec

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--text", required=True, help="Proposal text to search similar for")
    ap.add_argument("--topk", type=int, default=10)
    ap.add_argument("--model", default="sentence-transformers/all-MiniLM-L6-v2")
    ap.add_argument("--only-decided", action="store_true", help="Search only among decided apps")
    args = ap.parse_args()

    model = SentenceTransformer(args.model)
    q = model.encode([args.text], normalize_embeddings=True)[0].astype(np.float32)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        where = "WHERE a.council=? AND e.model_name=?"
        params = [COUNCIL, args.model]

        if args.only_decided:
            where += " AND a.decision IS NOT NULL AND a.decision<>''"

        rows = conn.execute(
            f"""
            SELECT a.id, a.application_ref, a.proposal, a.decision, a.decision_type, a.week_decided,
                   e.embedding, e.embedding_dim
            FROM applications a
            JOIN embeddings e ON e.application_id = a.id
            {where}
            """,
            params
        ).fetchall()

        if not rows:
            raise SystemExit("❌ No embedded rows found. Run build_embeddings_local.py first.")

        vecs = np.vstack([from_blob(r["embedding"], r["embedding_dim"]) for r in rows]).astype(np.float32)
        sims = cosine_sim_matrix(q, vecs)

        idx = np.argsort(-sims)[:args.topk]

        print("\n=== Top matches ===")
        results = []
        for rank, i in enumerate(idx, 1):
            r = rows[int(i)]
            score = float(sims[int(i)])
            results.append(r)
            print(f"\n{rank:02d}. score={score:.4f}  ref={r['application_ref']}  type={r['decision_type'] or ''}  week={r['week_decided'] or ''}")
            print(f"    decision: {r['decision'] or ''}")
            prop = (r["proposal"] or "").strip().replace("\n", " ")
            print(f"    proposal: {prop[:220]}{'...' if len(prop)>220 else ''}")

        # summary stats on topk
        counts = {}
        for r in results:
            dt = (r["decision_type"] or "").strip() or "unknown"
            counts[dt] = counts.get(dt, 0) + 1

        print("\n=== Decision-type breakdown (top matches) ===")
        for k, v in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])):
            print(f"{k}: {v}")

    finally:
        conn.close()

if __name__ == "__main__":
    main()
