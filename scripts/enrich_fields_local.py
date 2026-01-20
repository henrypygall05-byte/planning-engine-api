import argparse
import re
import sqlite3
from pathlib import Path

DB_PATH = Path("data/processed/planning.db")
COUNCIL = "Newcastle City Council"

WS_RE = re.compile(r"\s+")
PUNCT_RE = re.compile(r"[ \t]+")
HTML_ENT_RE = re.compile(r"&[a-zA-Z]+;")

def clean_text(s: str) -> str:
    s = (s or "").strip()
    s = s.replace("\u00ad", "")  # soft hyphen
    s = s.replace("\u200b", "")  # zero width space
    s = HTML_ENT_RE.sub(" ", s)
    s = WS_RE.sub(" ", s)
    return s.strip()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=50000)
    args = ap.parse_args()

    if not DB_PATH.exists():
        raise SystemExit(f"❌ DB not found: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    try:
        rows = conn.execute(
            """
            SELECT id, proposal
            FROM applications
            WHERE council = ?
              AND proposal IS NOT NULL
              AND proposal <> ''
            LIMIT ?
            """,
            (COUNCIL, args.limit),
        ).fetchall()

        updated = 0
        for app_id, proposal in rows:
            cleaned = clean_text(proposal)
            if cleaned != proposal:
                conn.execute(
                    "UPDATE applications SET proposal = ? WHERE id = ?",
                    (cleaned, app_id),
                )
                updated += 1

        conn.commit()
        print("✅ DONE")
        print("Rows scanned:", len(rows))
        print("Rows updated:", updated)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
