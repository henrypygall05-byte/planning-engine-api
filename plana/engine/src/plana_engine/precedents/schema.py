import os
import sqlite3
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS precedent_docs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  authority TEXT NOT NULL,
  case_key TEXT NOT NULL,
  reference TEXT,
  address TEXT,
  proposal TEXT,
  decision TEXT,
  decision_date TEXT,
  doc_title TEXT,
  source_path TEXT,
  created_at TEXT DEFAULT (datetime('now')),
  UNIQUE(authority, case_key)
);

CREATE TABLE IF NOT EXISTS precedent_chunks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  authority TEXT NOT NULL,
  case_key TEXT NOT NULL,
  doc_title TEXT,
  source_path TEXT,
  chunk_index INTEGER NOT NULL,
  page_start INTEGER,
  page_end INTEGER,
  paragraph_ref TEXT,
  text TEXT NOT NULL,
  created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_precedent_docs_lookup
  ON precedent_docs(authority, case_key);

CREATE INDEX IF NOT EXISTS idx_precedent_chunks_case
  ON precedent_chunks(authority, case_key);

CREATE INDEX IF NOT EXISTS idx_precedent_chunks_text
  ON precedent_chunks(text);
"""

def db_path() -> str:
    return os.environ.get("PLANA_DB_PATH", "../data/plana.sqlite")

def main():
    p = db_path()
    Path(p).parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(p)
    con.executescript(SCHEMA)
    con.commit()
    con.close()
    print(f"Precedent tables ready: {p}")

if __name__ == "__main__":
    main()
