#!/usr/bin/env python3
import argparse
import json
import os
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import faiss
from dotenv import load_dotenv

# --------- Helpers ---------

def load_env():
    # Explicit path avoids dotenv AssertionError in heredocs / stdin execution
    load_dotenv(dotenv_path=Path(".") / ".env")

def norm_text(s: str) -> str:
    return " ".join((s or "").strip().split())

def safe_float(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return default

def approval_bucket(rate: float) -> str:
    # Simple, explainable thresholds (council-safe)
    if rate >= 0.70:
        return "high"
    if rate >= 0.45:
        return "medium"
    return "low"

def decision_is_approved(decision: str) -> bool:
    d = (decision or "").lower()
    return any(k in d for k in ["approve", "approved", "grant", "granted", "permit", "permitted"])

def decision_is_refused(decision: str) -> bool:
    d = (decision or "").lower()
    return "refus" in d or "refuse" in d or "refused" in d

def infer_policy_conflicts(proposal: str) -> List[str]:
    """
    Lightweight heuristic policy flags (NOT a legal determination).
    This is just for an early pilot until policy docs are integrated.
    """
    p = (proposal or "").lower()
    flags = []
    if any(k in p for k in ["hmo", "house in multiple occupation", "bedsit", "multi-occup", "shared house"]):
        flags.append("HMO/intensification: check local HMO policies, amenity, waste storage, management plan.")
    if any(k in p for k in ["rear extension", "two storey", "2 storey", "first floor", "dormer", "loft"]):
        flags.append("Design/amenity impacts: daylight/overlooking/scale and character.")
    if any(k in p for k in ["change of use", "convert", "conversion"]):
        flags.append("Change of use: assess suitability of location, noise/amenity, parking, character.")
    if any(k in p for k in ["parking", "car parking", "dropped kerb", "vehicle access"]):
        flags.append("Highways/parking: check parking standards, access safety, traffic impacts.")
    if any(k in p for k in ["tree", "trees", "tpo", "protected tree"]):
        flags.append("Trees: check TPO constraints / arboricultural assessment.")
    if any(k in p for k in ["listed", "conservation", "heritage"]):
        flags.append("Heritage: listed building / conservation area impacts, require heritage statement.")
    return flags

def infer_common_conditions(similar_rows: List[Dict[str, Any]]) -> List[str]:
    """
    We don't have conditions text yet (that comes from scraping decision notices).
    For now we output typical condition categories based on proposal keywords.
    """
    all_text = " ".join((r.get("proposal") or "") for r in similar_rows).lower()
    conditions = []
    if any(k in all_text for k in ["extension", "alteration", "elevation"]):
        conditions.append("Materials to match existing / sample panels (typical).")
        conditions.append("Approved plans / drawings to be complied with (typical).")
    if any(k in all_text for k in ["hmo", "multi occupation", "shared"]):
        conditions.append("Noise management / amenity safeguards (typical for HMO).")
        conditions.append("Waste storage provision details (typical).")
    if any(k in all_text for k in ["parking", "access", "vehicle"]):
        conditions.append("Parking/turning area retained and kept available (typical).")
    if any(k in all_text for k in ["cycle", "bike"]):
        conditions.append("Cycle storage details (typical).")
    return conditions[:10]

# --------- Data access ---------

@dataclass
class SimilarHit:
    rank: int
    score: float
    application_ref: str
    url: Optional[str] = None
    proposal: Optional[str] = None
    site_address: Optional[str] = None
    decision: Optional[str] = None
    received_date: Optional[str] = None

def open_db(db_path: str) -> sqlite3.Connection:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    return con

def fetch_application_rows(con: sqlite3.Connection, refs: List[str]) -> Dict[str, Dict[str, Any]]:
    if not refs:
        return {}
    qmarks = ",".join("?" for _ in refs)
    # Try to select common fields. If your schema differs, we gracefully fallback.
    candidates = [
        "application_ref",
        "proposal",
        "site_address",
        "decision",
        "decision_type",
        "status",
        "received_date",
        "valid_date",
        "url",
    ]
    # Discover which columns exist
    cur = con.cursor()
    cur.execute("PRAGMA table_info(applications)")
    cols = {r["name"] for r in cur.fetchall()}
    sel = [c for c in candidates if c in cols]
    if "application_ref" not in sel:
        raise RuntimeError("DB schema unexpected: 'application_ref' column not found in applications table.")
    sql = f"SELECT {', '.join(sel)} FROM applications WHERE application_ref IN ({qmarks})"
    cur.execute(sql, refs)
    out = {}
    for row in cur.fetchall():
        d = dict(row)
        out[d["application_ref"]] = d
    return out

def load_meta(meta_path: str) -> List[Dict[str, Any]]:
    meta = []
    with open(meta_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            meta.append(json.loads(line))
    return meta

def build_meta_ref_map(meta: List[Dict[str, Any]]) -> List[str]:
    """
    The meta.jsonl should align 1:1 with FAISS vectors.
    We expect each meta row to include "application_ref".
    """
    refs = []
    for i, m in enumerate(meta):
        r = m.get("application_ref") or m.get("ref") or m.get("application_reference_number")
        if not r:
            raise RuntimeError(f"meta.jsonl row {i} missing application_ref/ref field.")
        refs.append(str(r))
    return refs

# --------- Embeddings ---------

def embed_text(text: str, model_name: str) -> np.ndarray:
    from sentence_transformers import SentenceTransformer
    model = SentenceTransformer(model_name)
    vec = model.encode([text], normalize_embeddings=True)
    return np.asarray(vec, dtype="float32")

# --------- Main reasoning ---------

def run_reasoning(
    proposal: str,
    address: str,
    use_class: str,
    k: int,
    db_path: str,
    faiss_path: str,
    meta_path: str,
    model_name: str,
) -> Dict[str, Any]:
    query_text = norm_text(f"Use class: {use_class}. Proposal: {proposal}. Address: {address}")
    q = embed_text(query_text, model_name)

    index = faiss.read_index(faiss_path)
    meta = load_meta(meta_path)
    meta_refs = build_meta_ref_map(meta)

    # Search
    D, I = index.search(q, k)  # D: similarity (if index built with inner product), I: ids
    ids = I[0].tolist()
    scores = D[0].tolist()

    # Map ids -> refs
    hit_refs = []
    for idx in ids:
        if idx < 0:
            continue
        if idx >= len(meta_refs):
            continue
        hit_refs.append(meta_refs[idx])

    con = open_db(db_path)
    rows_by_ref = fetch_application_rows(con, hit_refs)
    con.close()

    similar_cases: List[Dict[str, Any]] = []
    approved = 0
    refused = 0
    unknown = 0

    for n, (idx, score) in enumerate(zip(ids, scores), start=1):
        if idx < 0 or idx >= len(meta_refs):
            continue
        ref = meta_refs[idx]
        row = rows_by_ref.get(ref, {})
        decision = row.get("decision") or row.get("decision_type") or row.get("status") or ""
        if decision_is_approved(decision):
            approved += 1
        elif decision_is_refused(decision):
            refused += 1
        else:
            unknown += 1

        similar_cases.append({
            "rank": n,
            "score": round(safe_float(score), 4),
            "application_ref": ref,
            "proposal": row.get("proposal"),
            "site_address": row.get("site_address"),
            "decision": decision or None,
            "received_date": row.get("received_date") or row.get("valid_date"),
            "url": row.get("url"),
        })

    total_known = approved + refused
    approval_rate = (approved / total_known) if total_known else 0.5  # neutral if unknown

    common_conditions = infer_common_conditions(similar_cases)
    policy_conflicts = infer_policy_conflicts(proposal)

    # Evidence-backed reasoning text (simple + explainable)
    reasoning_lines = []
    reasoning_lines.append(f"Compared against {len(similar_cases)} similar Newcastle cases using text similarity (proposal + address + use class).")
    if total_known:
        reasoning_lines.append(f"Among similar cases with known outcomes: {approved} approved, {refused} refused (approval rate ~{approval_rate:.0%}).")
    else:
        reasoning_lines.append("Most similar cases do not yet have recorded outcomes in the database; treat likelihood as indicative only.")
    if policy_conflicts:
        reasoning_lines.append("Potential policy/assessment focus areas flagged from proposal keywords:")
        for f in policy_conflicts[:6]:
            reasoning_lines.append(f"- {f}")
    if common_conditions:
        reasoning_lines.append("Common condition themes seen across similar proposals:")
        for c in common_conditions[:6]:
            reasoning_lines.append(f"- {c}")

    output = {
        "input": {
            "proposal": proposal,
            "address": address,
            "use_class": use_class,
            "k": k,
        },
        "similar_cases": similar_cases,
        "summary": {
            "approved_count": approved,
            "refused_count": refused,
            "unknown_count": unknown,
            "approval_rate_estimate": round(float(approval_rate), 4),
            "approval_likelihood": approval_bucket(float(approval_rate)),
        },
        "common_conditions": common_conditions,
        "policy_conflicts": policy_conflicts,
        "reasoning": "\n".join(reasoning_lines),
        "notes": [
            "This is a pilot reasoning layer based on similarity + recorded fields in the DB.",
            "To make this officer-grade, next step is scraping decision notices to capture conditions and reasons verbatim.",
        ],
    }
    return output

def main():
    load_env()
    ap = argparse.ArgumentParser()
    ap.add_argument("--proposal", required=True)
    ap.add_argument("--address", required=True)
    ap.add_argument("--use-class", default="C3")
    ap.add_argument("--k", type=int, default=15)

    ap.add_argument("--db", default=os.getenv("DB_PATH", "./db/newcastle_planning.sqlite"))
    ap.add_argument("--faiss", default=os.getenv("FAISS_INDEX_PATH", "./index/app_index.faiss"))
    ap.add_argument("--meta", default=os.getenv("FAISS_META_PATH", "./index/meta.jsonl"))
    ap.add_argument("--embed-model", default=os.getenv("EMBED_MODEL", "sentence-transformers/all-MiniLM-L6-v2"))

    ap.add_argument("--out", default="data/out/case_officer_reasoning.json")

    args = ap.parse_args()

    out = run_reasoning(
        proposal=args.proposal,
        address=args.address,
        use_class=args.use_class,
        k=args.k,
        db_path=args.db,
        faiss_path=args.faiss,
        meta_path=args.meta,
        model_name=args.embed_model,
    )

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"OK: wrote {out_path}")

    # Also print a short console summary
    print("\n--- SUMMARY ---")
    print(out["summary"])
    print("\n--- TOP 5 SIMILAR ---")
    for c in out["similar_cases"][:5]:
        print(f"{c['rank']:02d}. score={c['score']} ref={c['application_ref']} decision={c.get('decision')} url={c.get('url')}")

if __name__ == "__main__":
    main()
