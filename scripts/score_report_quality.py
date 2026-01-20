#!/usr/bin/env python3
import json
import sys
from collections import Counter

C3_TERMS = {
    "c3","dwellinghouse","dwelling","residential","housing","amenity","privacy",
    "outlook","noise","refuse","cycle","parking","design","character","heritage",
    "conservation","materials","windows","doors","floor","internal","alterations",
    "change of use","use class"
}

BAD_SIGNALS = {
    "leisure","tourism","nightclub","cinema","museum","arena"
}

def norm(s: str) -> str:
    return (s or "").lower()

def term_hits(text: str, terms: set[str]) -> int:
    t = norm(text)
    hits = 0
    for w in terms:
        if w in t:
            hits += 1
    return hits

def main():
    if len(sys.argv) < 2:
        print("Usage: score_report_quality.py <payload.json>")
        return 2

    path = sys.argv[1]
    try:
        payload = json.loads(open(path, "r", encoding="utf-8").read())
    except Exception as e:
        print(f"FAIL: could not parse JSON: {e}")
        return 1

    ev = payload.get("policy", {}).get("evidence", []) or []
    cits = payload.get("policy", {}).get("citations", []) or []
    decision = payload.get("report", {}).get("decision")

    # Document diversity
    doc_keys = [c.get("doc_key") for c in cits if c.get("doc_key")]
    doc_div = len(set(doc_keys))

    # Relevance scoring using evidence snippets/text
    rel_scores = []
    bad_scores = []
    for e in ev[:10]:
        blob = " ".join([e.get("snippet",""), e.get("text","")])
        rel_scores.append(term_hits(blob, C3_TERMS))
        bad_scores.append(term_hits(blob, BAD_SIGNALS))

    rel_sum = sum(rel_scores)
    bad_sum = sum(bad_scores)

    # Simple composite score
    score = 50
    score += min(30, rel_sum * 2)          # reward relevance
    score += min(10, doc_div * 3)          # reward diversity
    score -= min(30, bad_sum * 5)          # penalize obviously irrelevant topics

    score = max(0, min(100, score))

    print("== Report Quality Check ==")
    print(f"Payload: {path}")
    print(f"Decision: {decision}")
    print(f"Docs in top citations: {doc_div} ({', '.join(sorted(set(doc_keys)))})")
    print(f"C3 relevance hits (top evidence): {rel_scores}  (sum={rel_sum})")
    if bad_sum:
        print(f"⚠️ Irrelevance signals detected: {bad_scores} (sum={bad_sum})")
    print(f"Quality score (heuristic): {score}/100")

    # Warnings
    warnings = []
    if doc_div < 2:
        warnings.append("Low document diversity in top citations (only 1 doc).")
    if rel_sum < 5:
        warnings.append("Low C3 relevance in top evidence (consider tuning retrieval).")
    if bad_sum >= 1:
        warnings.append("Some evidence looks unrelated (e.g., leisure/tourism/etc.).")

    if warnings:
        print("\nWarnings:")
        for w in warnings:
            print(f"- {w}")
    else:
        print("\nLooks good ✅")

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
