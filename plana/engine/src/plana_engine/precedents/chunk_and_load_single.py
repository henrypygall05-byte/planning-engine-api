import os
import re
import sqlite3
from pathlib import Path
from typing import List, Tuple

PAGE_RE = re.compile(r"=== PAGE (\d+) ===")

def db_path() -> str:
    return os.environ.get("PLANA_DB_PATH", "../data/plana.sqlite")

def split_pages(raw: str) -> List[Tuple[int, str]]:
    parts = PAGE_RE.split(raw)
    if len(parts) < 3:
        return []
    out = []
    i = 1
    while i < len(parts):
        pno = int(parts[i])
        txt = parts[i+1]
        out.append((pno, txt.strip()))
        i += 2
    return out

def chunk_pages(pages: List[Tuple[int, str]], max_chars: int = 3500) -> List[Tuple[int, int, int, str]]:
    chunks = []
    idx = 0
    for pno, txt in pages:
        if not txt:
            continue
        if len(txt) <= max_chars:
            chunks.append((idx, pno, pno, txt))
            idx += 1
            continue
        start = 0
        while start < len(txt):
            end = min(start + max_chars, len(txt))
            chunk = txt[start:end].strip()
            if chunk:
                chunks.append((idx, pno, pno, chunk))
                idx += 1
            start = end
    return chunks

def upsert_doc(con, authority, case_key, reference, address, proposal, decision, decision_date, doc_title, source_path):
    con.execute("""
    INSERT INTO precedent_docs(authority, case_key, reference, address, proposal, decision, decision_date, doc_title, source_path)
    VALUES(?,?,?,?,?,?,?,?,?)
    ON CONFLICT(authority, case_key) DO UPDATE SET
      reference=excluded.reference,
      address=excluded.address,
      proposal=excluded.proposal,
      decision=excluded.decision,
      decision_date=excluded.decision_date,
      doc_title=excluded.doc_title,
      source_path=excluded.source_path
    """, (authority, case_key, reference, address, proposal, decision, decision_date, doc_title, source_path))

def clear_chunks(con, authority, case_key):
    con.execute("DELETE FROM precedent_chunks WHERE authority=? AND case_key=?", (authority, case_key))

def insert_chunk(con, authority, case_key, doc_title, source_path, chunk_index, page_start, page_end, text):
    paragraph_ref = f"pp.{page_start}-{page_end}#c{chunk_index}"
    con.execute("""
    INSERT INTO precedent_chunks(authority, case_key, doc_title, source_path, chunk_index, page_start, page_end, paragraph_ref, text)
    VALUES(?,?,?,?,?,?,?,?,?)
    """, (authority, case_key, doc_title, source_path, chunk_index, page_start, page_end, paragraph_ref, text))

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--authority", required=True)
    ap.add_argument("--case_key", required=True)
    ap.add_argument("--doc_title", required=True)
    ap.add_argument("--source_pdf", required=True)
    ap.add_argument("--text_file", required=True)

    ap.add_argument("--reference", default="")
    ap.add_argument("--address", default="")
    ap.add_argument("--proposal", default="")
    ap.add_argument("--decision", default="")
    ap.add_argument("--decision_date", default="")
    args = ap.parse_args()

    con = sqlite3.connect(db_path())

    upsert_doc(
        con,
        args.authority,
        args.case_key,
        args.reference,
        args.address,
        args.proposal,
        args.decision,
        args.decision_date,
        args.doc_title,
        args.source_pdf,
    )
    clear_chunks(con, args.authority, args.case_key)

    raw = Path(args.text_file).read_text(encoding="utf-8", errors="ignore")
    pages = split_pages(raw)
    if not pages:
        raise SystemExit("No page markers found (=== PAGE N ===). Extraction likely failed.")

    for idx, p1, p2, txt in chunk_pages(pages):
        insert_chunk(con, args.authority, args.case_key, args.doc_title, args.source_pdf, idx, p1, p2, txt)

    con.commit()
    con.close()
    print(f"Loaded precedent chunks: {args.authority}/{args.case_key}")

if __name__ == "__main__":
    main()
