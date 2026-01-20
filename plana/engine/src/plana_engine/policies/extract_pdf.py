from pathlib import Path
from pypdf import PdfReader

def extract(pdf_path: Path, out_txt: Path) -> None:
    r = PdfReader(str(pdf_path))
    out_txt.parent.mkdir(parents=True, exist_ok=True)
    parts = []
    for i, page in enumerate(r.pages, start=1):
        parts.append(f"\n\n=== PAGE {i} ===\n")
        parts.append(page.extract_text() or "")
    out_txt.write_text("\n".join(parts), encoding="utf-8")

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--in_pdf", required=True)
    ap.add_argument("--out_txt", required=True)
    args = ap.parse_args()
    extract(Path(args.in_pdf), Path(args.out_txt))
    print(f"Extracted -> {args.out_txt}")

if __name__ == "__main__":
    main()
