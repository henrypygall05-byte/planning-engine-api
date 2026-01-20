#!/usr/bin/env python3
import argparse
import json
from datetime import datetime
from pathlib import Path

def clamp(x, lo, hi):
    return max(lo, min(hi, x))

def parse_quality_score(text: str):
    # expects: 'Quality score (heuristic): 49/100'
    import re
    for line in text.splitlines():
        m = re.search(r"Quality score\s*\(heuristic\)\s*:\s*(\d+)\s*/\s*100", line, re.I)
        if m:
            return int(m.group(1))
    return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--score", required=True, help="Path to score_*.txt produced by quality checker")
    ap.add_argument("--payload", required=True, help="Path to payload_*.json")
    ap.add_argument("--weights", default="config/relevance_weights.json", help="Weights json path")
    ap.add_argument("--log", default="logs/feedback/usage_log.jsonl", help="Usage log jsonl path")
    args = ap.parse_args()

    score_path = Path(args.score)
    payload_path = Path(args.payload)
    weights_path = Path(args.weights)
    log_path = Path(args.log)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    # load files
    w = json.loads(weights_path.read_text(encoding="utf-8")) if weights_path.exists() else {}
    payload = json.loads(payload_path.read_text(encoding="utf-8"))

    t = score_path.read_text(encoding="utf-8", errors="ignore")
    tl = t.lower()

    quality = parse_quality_score(t)
    low_div = "low document diversity" in tl
    irrelevant = ("irrelevance signals detected" in tl) or ("some evidence looks unrelated" in tl)

    doc_boost = w.setdefault("doc_boost", {})
    topic_penalty = w.setdefault("topic_penalty", {})

    def bump(doc, delta):
        doc_boost[doc] = clamp(float(doc_boost.get(doc, 1.0)) + float(delta), 0.80, 1.50)

    if low_div:
        bump("nppf_2024", +0.03)
        bump("dap_2020", +0.02)
        bump("csucp_2015", -0.02)

    if irrelevant:
        for k in ["leisure", "tourism", "nightclub", "employment land", "retail hierarchy"]:
            topic_penalty[k] = clamp(float(topic_penalty.get(k, 0.90)) - 0.03, 0.50, 1.00)

    weights_path.write_text(json.dumps(w, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    row = {
        "ts": datetime.utcnow().isoformat() + "Z",
        "proposal": payload.get("input", {}).get("proposal_text"),
        "decision": payload.get("report", {}).get("decision"),
        "quality_score": quality,
        "flags": {
            "low_doc_diversity": low_div,
            "irrelevance": irrelevant
        },
        "weights_after": w
    }

    with log_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")

    print(f"OK: updated {weights_path} and appended {log_path}")

if __name__ == "__main__":
    main()
