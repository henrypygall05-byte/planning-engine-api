import sqlite3
from pathlib import Path

DB_PATH = Path("data/processed/planning.db")

SCHEMA_SQL = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS applications (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    council TEXT NOT NULL,
    application_ref TEXT NOT NULL,

    address TEXT,
    postcode TEXT,

    proposal TEXT,
    decision TEXT,
    decision_type TEXT,

    date_received TEXT,
    date_decided TEXT,

    raw_json TEXT,

    created_at TEXT DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(council, application_ref)
);

CREATE INDEX IF NOT EXISTS idx_applications_ref
    ON applications(application_ref);

CREATE INDEX IF NOT EXISTS idx_applications_decision
    ON applications(decision_type);
"""
def main():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    try:
        conn.executescript(SCHEMA_SQL)
        conn.commit()
    finally:
        conn.close()

    print("✅ Database initialised at:", DB_PATH.resolve())

if __name__ == "__main__":
    main()
