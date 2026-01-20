import os
import sys
import json
from typing import Any, Dict, List, Optional

# --- bootstrap: ensure src/ is importable when running as module or script ---
THIS_FILE = os.path.abspath(__file__)
SRC_DIR = os.path.dirname(os.path.dirname(os.path.dirname(THIS_FILE)))  # .../src
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from plana_engine.policies.retrieve_policies import retrieve_policies


def generate_report_payload(
    proposal_text: str,
    authority: str = "newcastle",
    doc_keys: Optional[List[str]] = None,
    top_k: int = 10,
    min_score: float = 2.0,
) -> Dict[str, Any]:
    """
    Stage 3: Report payload generator (clean, deterministic)
    - Retrieves policy evidence
    - Enforces no answer without citations
    - Returns structured payload for UI / LLM layer
    """

    if not proposal_text or not proposal_text.strip():
        return {"ok": False, "reason": "empty_proposal", "report": None}

    if not doc_keys:
        doc_keys = ["dap_2020", "csucp_2015", "nppf_2024"]

    policy_block = retrieve_policies(
        query=proposal_text,
        top_k=top_k,
        authority=authority,
        doc_keys=doc_keys,
        min_score=min_score,
    )

    # Enforce: no answer without citations
    if not policy_block.get("ok") or not policy_block.get("results"):
        return {
            "ok": False,
            "reason": "insufficient_policy_evidence",
            "input": {
                "proposal_text": proposal_text,
                "authority": authority,
                "doc_keys": doc_keys,
            },
            "policy": {
                "ok": False,
                "citations": [],
                "evidence": [],
            },
            "report": None,
        }

    results = policy_block["results"]

    citations = [
        {
            "authority": r["authority"],
            "doc_key": r["doc_key"],
            "doc_title": r["doc_title"],
            "paragraph_ref": r["paragraph_ref"],
            "page_start": r["page_start"],
            "page_end": r["page_end"],
            "source_path": r["source_path"],
            "score": r["score"],
        }
        for r in results
    ]

    avg_score = sum(float(r["score"]) for r in results) / len(results)

    if avg_score >= 3.5:
        decision = "approve_with_conditions"
        conditions = [
            "Materials to match the existing dwelling unless otherwise agreed in writing.",
            "Development to be carried out in accordance with approved plans.",
        ]
    else:
        decision = "needs_officer_review"
        conditions = []

    report = {
        "decision": decision,
        "confidence": round(min(avg_score / 6.0, 1.0), 2),
        "summary": "Preliminary recommendation based on policy evidence. Subject to full officer judgement.",
        "draft_conditions": conditions,
        "note": "Deterministic Stage 3 output. Precedents + LLM reasoning added in Stage 4.",
    }

    return {
        "ok": True,
        "reason": None,
        "input": {
            "proposal_text": proposal_text,
            "authority": authority,
            "doc_keys": doc_keys,
        },
        "signals": {
            "policy_avg_score": avg_score,
            "policy_count": len(results),
        },
        "policy": {
            "ok": True,
            "citations": citations,
            "evidence": results,
        },
        "report": report,
    }


def main():
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--proposal", required=True)
    ap.add_argument("--authority", default="newcastle")
    ap.add_argument("--doc_keys", default="dap_2020,csucp_2015,nppf_2024")
    ap.add_argument("--top_k", type=int, default=10)
    ap.add_argument("--min_score", type=float, default=2.0)
    args = ap.parse_args()

    doc_keys = [d.strip() for d in args.doc_keys.split(",") if d.strip()]

    payload = generate_report_payload(
        proposal_text=args.proposal,
        authority=args.authority,
        doc_keys=doc_keys,
        top_k=args.top_k,
        min_score=args.min_score,
    )

    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
