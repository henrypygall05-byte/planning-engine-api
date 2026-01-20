import sqlite3
from pathlib import Path

DB_PATH = Path("data/processed/planning.db")

def main():
    conn = sqlite3.connect(DB_PATH)
    try:
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
        print("✅ embeddings table ready")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
