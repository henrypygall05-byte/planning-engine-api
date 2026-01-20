import os
import sqlite3
from pathlib import Path

def db_path() -> str:
    return os.getenv(
        "PLANA_DB_PATH",
        str(Path(__file__).resolve().parents[3] / "data" / "plana.sqlite")
    )

SCHEMA = """
CREATE TABLE IF NOT EXISTS policy_documents (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  authority TEXT NOT NULL,
  doc_key TEXT NOT NULL,
  doc_title TEXT NOT NULL,
  source_path TEXT NOT NULL,
  version_label TEXT,
  created_at TEXT DEFAULT (datetime('now')),
  UNIQUE(authority, doc_key)
);

CREATE TABLE IF NOT EXISTS policy_chunks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  authority TEXT NOT NULL,
  doc_key TEXT NOT NULL,
  doc_title TEXT NOT NULL,
  source_path TEXT NOT NULL,
  page_start INTEGER,
  page_end INTEGER,
  section_path TEXT,
  paragraph_ref TEXT,
  chunk_index INTEGER NOT NULL,
  text TEXT NOT NULL,
  created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_policy_chunks_doc
  ON policy_chunks(authority, doc_key);

CREATE INDEX IF NOT EXISTS idx_policy_chunks_text
  ON policy_chunks(text);
"""

def main():
    p = db_path()
    Path(p).parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(p)
    con.executescript(SCHEMA)
    con.commit()
    con.close()
    print(f"Policy tables ready: {p}")

if __name__ == "__main__":
    main()
