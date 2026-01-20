import os, re, sqlite3
from pathlib import Path

PAGE_RE = re.compile(r"^=== PAGE (\d+) ===$", re.MULTILINE)

def split_pages(text: str):
    ms = list(PAGE_RE.finditer(text))
    out = []
    for i, m in enumerate(ms):
        p = int(m.group(1))
        start = m.end()
        end = ms[i+1].start() if i+1 < len(ms) else len(text)
        out.append((p, text[start:end].strip()))
    return out

def chunk_pages(pages, max_chars=1500):
    idx = 0
    for p, txt in pages:
        if not txt:
            continue
        for i in range(0, len(txt), max_chars):
            yield idx, p, p, txt[i:i+max_chars].strip()
            idx += 1

def db_path():
    return os.getenv(
        "PLANA_DB_PATH",
        str(Path(__file__).resolve().parents[3] / "data" / "plana.sqlite")
    )

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--authority", required=True)
    ap.add_argument("--doc_key", required=True)
    ap.add_argument("--doc_title", required=True)
    ap.add_argument("--source_pdf", required=True)
    ap.add_argument("--text_file", required=True)
    ap.add_argument("--version_label", default=None)
    args = ap.parse_args()

    con = sqlite3.connect(db_path())
    con.execute("PRAGMA journal_mode=DELETE;")
    con.execute("PRAGMA synchronous=FULL;")
    con.execute("PRAGMA busy_timeout=5000;")

    con.execute("""
      INSERT INTO policy_documents(authority, doc_key, doc_title, source_path, version_label)
      VALUES (?, ?, ?, ?, ?)
      ON CONFLICT(authority, doc_key) DO UPDATE SET
        doc_title=excluded.doc_title,
        source_path=excluded.source_path,
        version_label=excluded.version_label
    """, (args.authority, args.doc_key, args.doc_title, args.source_pdf, args.version_label))

    con.execute("DELETE FROM policy_chunks WHERE authority=? AND doc_key=?", (args.authority, args.doc_key))

    raw = Path(args.text_file).read_text(encoding="utf-8", errors="ignore")
    pages = split_pages(raw)

    for idx, p1, p2, txt in chunk_pages(pages):
        con.execute("""
          INSERT INTO policy_chunks(
            authority, doc_key, doc_title, source_path,
            page_start, page_end, section_path, paragraph_ref,
            chunk_index, text
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            args.authority, args.doc_key, args.doc_title, args.source_pdf,
            p1, p2, None, f"pp.{p1}-{p2}#c{idx}",
            idx, txt
        ))

    con.commit()
    con.close()
    print(f"Loaded chunks safely: {args.authority}/{args.doc_key}")

if __name__ == "__main__":
    main()
