import sqlite3
import argparse
import numpy as np
from sentence_transformers import SentenceTransformer

DB_PATH = "data/processed/planning.db"
COUNCIL = "Newcastle City Council"

def to_blob(vec: np.ndarray) -> bytes:
    # float32 bytes
    return vec.astype(np.float32).tobytes()

def ensure_table(conn: sqlite3.Connection):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS embeddings (
        application_id INTEGER PRIMARY KEY,
        embedding BLOB NOT NULL,
        embedding_dim INTEGER NOT NULL,
        model_name TEXT NOT NULL,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(application_id) REFERENCES applications(id)
    );
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_embeddings_model ON embeddings(model_name);")
    conn.commit()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0, help="0 means no limit")
    ap.add_argument("--only-decided", action="store_true", help="Only embed decided apps")
    ap.add_argument("--model", default="sentence-transformers/all-MiniLM-L6-v2")
    ap.add_argument("--batch-size", type=int, default=64)
    args = ap.parse_args()

    model = SentenceTransformer(args.model)
    model_name = args.model

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        ensure_table(conn)

        where = "WHERE council=? AND proposal IS NOT NULL AND proposal<>''"
        params = [COUNCIL]

        if args.only_decided:
            where += " AND decision IS NOT NULL AND decision<>''"

        # Skip rows already embedded for this model
        query = f"""
        SELECT a.id, a.proposal
        FROM applications a
        LEFT JOIN embeddings e
          ON e.application_id = a.id AND e.model_name = ?
        {where} AND e.application_id IS NULL
        ORDER BY a.id ASC
        """
        params2 = [model_name] + params

        if args.limit and args.limit > 0:
            query += " LIMIT ?"
            params2.append(args.limit)

        rows = conn.execute(query, params2).fetchall()
        total = len(rows)
        print("Rows to embed:", total)

        if total == 0:
            print("✅ Nothing to do.")
            return

        # Batch encode
        changed = 0
        bs = args.batch_size
        for i in range(0, total, bs):
            batch = rows[i:i+bs]
            texts = [r["proposal"] for r in batch]
            vecs = model.encode(texts, show_progress_bar=False, normalize_embeddings=True)
            vecs = np.asarray(vecs, dtype=np.float32)

            for r, v in zip(batch, vecs):
                conn.execute(
                    """
                    INSERT INTO embeddings (application_id, embedding, embedding_dim, model_name)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(application_id) DO UPDATE SET
                      embedding=excluded.embedding,
                      embedding_dim=excluded.embedding_dim,
                      model_name=excluded.model_name
                    """,
                    (r["id"], to_blob(v), int(v.shape[0]), model_name),
                )
                changed += 1

            conn.commit()
            print(f"Embedded {min(i+bs,total)}/{total}")

        print("✅ DONE. Rows embedded:", changed)

    finally:
        conn.close()

if __name__ == "__main__":
    main()
