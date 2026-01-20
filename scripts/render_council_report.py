#!/usr/bin/env python3
import argparse
import json
import os
import subprocess
from datetime import datetime
from pathlib import Path


# Optional reranker (doc diversity + topic filtering)
try:
    from scripts.report_rerank import rerank_policy
except Exception:
    rerank_policy = None
def run_precedents(proposal_text: str):
    """
    Calls scripts/get_precedents.py if available and returns a list[dict] like:
    [{rank, score, meta:{...}}, ...]
    """
    gp = Path("scripts/get_precedents.py")
    if not gp.exists():
        return []

    # Use same python interpreter running this script
    cmd = ["python3", str(gp), proposal_text]
    env = os.environ.copy()
    # ensure default path points at your built index
    env.setdefault("FAISS_INDEX_PATH", "./index/app_index.faiss")
    env.setdefault("FAISS_META_PATH", "./index/app_index_meta.json")

    try:
        out = subprocess.check_output(cmd, env=env, stderr=subprocess.STDOUT, text=True)
        data = json.loads(out)
        if isinstance(data, list):
            return data
        return []
    except Exception:
        return []

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("payload_json", help="Path to payload JSON")
    ap.add_argument("-o", "--out", default="logs/report_latest.md", help="Output markdown path")
    ap.add_argument("--include-precedents", action="store_true", help="Try to include similar applications")
    args = ap.parse_args()

    payload_path = Path(args.payload_json)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    data = json.loads(payload_path.read_text(encoding="utf-8"))

    proposal = data.get("input", {}).get("proposal_text", "[AUTO]")
    authority = data.get("input", {}).get("authority", "newcastle")
    doc_keys = data.get("input", {}).get("doc_keys", [])
    signals = data.get("signals", {})
    policy = data.get("policy", {})
    report = data.get("report", {})

    citations = policy.get("citations", [])
    evidence = policy.get("evidence", [])

    # Load rerank weights (auto-tuned over time)
    weights_path = Path("config/relevance_weights.json")
    weights = {}
    if weights_path.exists():
        try:
            weights = json.loads(weights_path.read_text(encoding="utf-8"))
        except Exception:
            weights = {}

    # Rerank + enforce doc diversity so NPPF + DAP can surface
    if rerank_policy is not None:
        citations, evidence = rerank_policy(citations, evidence, weights)

    decision = report.get("decision", "needs_officer_review")
    confidence = report.get("confidence", "")
    summary = report.get("summary", "")
    conditions = report.get("draft_conditions", [])

    today = datetime.now().strftime("%Y-%m-%d")

    md = []
    md.append("# CASE OFFICER REPORT (DRAFT – PILOT, DETERMINISTIC)")
    md.append("")
    md.append("**Authority:** Newcastle City Council  ")
    md.append("**Application Ref:** [OFFICER REVIEW]  ")
    md.append("**Site Address:** [OFFICER REVIEW]  ")
    md.append(f"**Proposal:** {proposal}  ")
    md.append(f"**Date:** {today}  ")
    if confidence != "":
        md.append(f"**Engine Confidence (heuristic):** {confidence}  ")
    md.append("")
    md.append("---")
    md.append("")

    md.append("## 1.0 Site and Surroundings")
    md.append("[OFFICER REVIEW] Describe site context, planning history, constraints (conservation area/listed/building regs), and surrounding uses.")
    md.append("")

    md.append("## 2.0 The Proposal")
    md.append(proposal)
    md.append("")

    md.append("## 3.0 Policy Context")
    md.append("### 3.1 Documents queried (pilot)")
    md.append(f"- Authority: `{authority}`")
    if doc_keys:
        md.append("- Documents: " + ", ".join(f"`{k}`" for k in doc_keys))
    md.append("")

    md.append("### 3.2 Retrieved citations (top matches)")
    if citations:
        for c in citations:
            md.append(
                f"- **{c.get('doc_title','')}** ({c.get('doc_key','')}) — {c.get('paragraph_ref','')} "
                f"(pp.{c.get('page_start','')}-{c.get('page_end','')}) — score {c.get('score','')}"
            )
    else:
        md.append("- None returned.")
    md.append("")

    md.append("### 3.3 Evidence excerpts (traceability)")
    if evidence:
        for e in evidence[:8]:
            md.append(f"**{e.get('doc_title','')}** — {e.get('paragraph_ref','')} (score {e.get('score','')})")
            snippet = (e.get("snippet") or e.get("text") or "").strip()
            if snippet:
                md.append("> " + "\n> ".join(snippet.splitlines()[:12]))
            md.append(f"*(Source: {e.get('source_path','')}; pages {e.get('page_start','')}-{e.get('page_end','')})*")
            md.append("")
    else:
        md.append("- None returned.")
        md.append("")

    # --- Precedents ---
    md.append("## 3.4 Similar applications (precedents)")
    if args.include_precedents:
        precedents = run_precedents(proposal)
        if precedents:
            for pr in precedents:
                meta = pr.get("meta") or {}
                ref = meta.get("reference") or meta.get("app_ref") or meta.get("application_ref") or meta.get("uid") or meta.get("id") or "[UNKNOWN REF]"
                addr = meta.get("address") or meta.get("site") or meta.get("site_address") or meta.get("location") or ""
                decision_meta = meta.get("decision") or meta.get("outcome") or meta.get("status") or ""
                md.append(f"- **{ref}** — {addr}".rstrip())
                if decision_meta:
                    md.append(f"  - Decision/Status: {decision_meta}")
                md.append(f"  - Similarity score: {pr.get('score')}")
        else:
            md.append("- No precedents returned (index not found or query failed).")
    else:
        md.append("- Not requested. Re-run with `--include-precedents` to include similar applications.")
    md.append("")

    md.append("## 4.0 Assessment (Pilot Draft)")
    md.append("> Pilot note: This draft is generated using policy retrieval + deterministic scoring. It does not ingest consultations/site constraints unless provided.")
    md.append("")

    md.append("### 4.1 Principle of Development (C3 Residential)")
    md.append("- The proposal is assessed against the Development Plan and the NPPF evidence identified above.")
    md.append("- Officer review required for site constraints, lawful use position, and any GPDO considerations.")
    md.append("")

    md.append("### 4.2 Key signals from the pilot run")
    if signals:
        md.append(f"- Policy matches returned: **{signals.get('policy_count','')}**")
        md.append(f"- Average policy score: **{signals.get('policy_avg_score','')}**")
    else:
        md.append("- No signals returned.")
    md.append("")

    md.append("## 5.0 Planning Balance and Conclusion")
    md.append("### 5.1 Benefits")
    md.append("- [AUTO] Alignment with identified policy objectives (subject to officer review).")
    md.append("- [OFFICER REVIEW] Any specific housing benefits / reuse of building / regeneration benefits.")
    md.append("")

    md.append("### 5.2 Harms / Risks / Uncertainties")
    md.append("- [OFFICER REVIEW] Amenity, highways, bin/cycle storage, heritage, design, flooding etc. (pilot needs extra inputs to assess these).")
    md.append("")

    md.append("### 5.3 Overall conclusion")
    md.append(summary if summary else "[AUTO] Conclusion to be finalised by officer.")
    md.append("")

    md.append("## 6.0 Recommendation")
    md.append(f"**Recommended decision:** **{decision}**")
    md.append("")

    if "approve" in str(decision).lower():
        md.append("### 6.1 Conditions (Draft – Officer Review)")
        if conditions:
            for i, cond in enumerate(conditions, 1):
                md.append(f"{i}. {cond}")
        else:
            md.append("1. Time limit (3 years).")
            md.append("2. Approved plans.")
        md.append("")
    elif "refuse" in str(decision).lower():
        md.append("### 6.1 Reasons for Refusal (Draft – Officer Review)")
        md.append("- [AUTO] Insert reasons linked to the policy conflicts evidenced above.")
        md.append("")

    md.append("## 7.0 Evidence Appendix (Traceability)")
    md.append(f"- Payload source: `{payload_path}`")
    md.append("")

    out_path.write_text("\n".join(md).strip() + "\n", encoding="utf-8")
    print(f"OK: wrote {out_path}")

if __name__ == "__main__":
    main()
