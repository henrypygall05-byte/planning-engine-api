#!/usr/bin/env python3
import argparse
import json
from datetime import datetime
from pathlib import Path

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--payload", required=True)
    ap.add_argument("--score", required=True)
    ap.add_argument("--out", default="logs/feedback/feedback.jsonl")
    args = ap.parse_args()

    payload_p = Path(args.payload)
    score_p = Path(args.score)
    out_p = Path(args.out)
    out_p.parent.mkdir(parents=True, exist_ok=True)

    payload = json.loads(payload_p.read_text(encoding="utf-8"))

    score_txt = score_p.read_text(encoding="utf-8", errors="ignore")
    # keep it simple: store the whole score file text + key fields from payload
    rec = {
        "ts": datetime.now().isoformat(timespec="seconds"),
        "proposal_text": payload.get("input", {}).get("proposal_text"),
        "authority": payload.get("input", {}).get("authority"),
        "doc_keys": payload.get("input", {}).get("doc_keys"),
        "decision": payload.get("report", {}).get("decision"),
        "score_text": score_txt,
        "payload_path": str(payload_p),
        "score_path": str(score_p)
    }

    with out_p.open("a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    print(f"OK: appended feedback -> {out_p}")

if __name__ == "__main__":
    main()
