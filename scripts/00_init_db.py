import os
import sqlite3
from pathlib import Path
from dotenv import load_dotenv

# Repo root = parent of scripts/
ROOT = Path(__file__).resolve().parent.parent

# Always load .env from repo root
load_dotenv(dotenv_path=str(ROOT / ".env"))

# Read DB_PATH but make it safe
db_path_env = os.getenv("DB_PATH") or "db/newcastle_planning.sqlite"
db_path_env = db_path_env.strip()

# If DB_PATH is absolute, use it; otherwise make it relative to repo root
db_path = Path(db_path_env)
if not db_path.is_absolute():
    db_path = ROOT / db_path

# IMPORTANT: Always create the project's ./db folder (not dirname(DB_PATH))
(ROOT / "db").mkdir(parents=True, exist_ok=True)

print(">>> ROOT =", ROOT)
print(">>> DB_PATH (resolved) =", str(db_path))

con = sqlite3.connect(str(db_path))
cur = con.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS applications (
  application_ref TEXT PRIMARY KEY,
  site_address TEXT,
  address TEXT,
  proposal TEXT,
  status TEXT,
  decision TEXT,
  decision_type TEXT,
  received_date TEXT,
  validated_date TEXT,
  decision_date TEXT,
  ward TEXT,
  parish TEXT,
  case_officer TEXT,
  url TEXT,
  source TEXT,
  last_seen_utc TEXT,
  enriched_utc TEXT
);
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS embeddings (
  application_ref TEXT PRIMARY KEY,
  text_hash TEXT,
  embedded_utc TEXT
);
""")

con.commit()
con.close()

print("✅ OK: initialized DB at", str(db_path))
