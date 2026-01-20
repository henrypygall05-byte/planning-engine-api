#!/usr/bin/env python3
import argparse
import json
from pathlib import Path

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--feedback", default="logs/feedback/feedback.jsonl")
    ap.add_argument("--weights", default="config/relevance_weights.json")
    ap.add_argument("--min_records", type=int, default=3)
    args = ap.parse_args()

    feedback_p = Path(args.feedback)
    weights_p = Path(args.weights)

    if not weights_p.exists():
        raise SystemExit(f"Missing weights file: {weights_p}")

    weights = json.loads(weights_p.read_text(encoding="utf-8"))

    if not feedback_p.exists():
        print("No feedback yet; leaving weights unchanged.")
        return

    lines = feedback_p.read_text(encoding="utf-8").strip().splitlines()
    if len(lines) < args.min_records:
        print(f"Not enough feedback records ({len(lines)}) to auto-tune (min={args.min_records}).")
        return

    # Look at last N
    last = lines[-min(10, len(lines)):]
    txt = "\n".join(last).lower()

    changed = False

    # If we keep seeing doc-diversity warnings, increase target
    if "low document diversity" in txt:
        cur = int(weights.get("doc_diversity_target", 2))
        if cur < 3:
            weights["doc_diversity_target"] = cur + 1
            changed = True

    # If irrelevance flagged, increase penalty slightly
    if "irrelevance" in txt or "unrelated" in txt or "leisure/tourism" in txt:
        cur = float(weights.get("irrelevance_penalty_per_hit", 0.6))
        weights["irrelevance_penalty_per_hit"] = min(cur + 0.1, 1.5)
        changed = True

        # Also slightly increase leisure/tourism penalties
        tp = weights.get("topic_penalties", {})
        for k in ["leisure", "tourism", "nightlife"]:
            tp[k] = min(float(tp.get(k, 2.0)) + 0.2, 4.0)
        weights["topic_penalties"] = tp
        changed = True

    if changed:
        weights_p.write_text(json.dumps(weights, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        print(f"OK: updated weights -> {weights_p}")
    else:
        print("No change triggered by recent feedback.")

if __name__ == "__main__":
    main()
