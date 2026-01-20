#!/usr/bin/env python3
import argparse, json, subprocess
from datetime import datetime
from pathlib import Path

# Rerank policy evidence (doc diversity + relevance filtering)
try:
    from scripts.report_rerank import rerank_policy
except Exception:
    rerank_policy = None

def safe_json_load(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))

def load_weights():
    weights_path = Path("config/relevance_weights.json")
    if weights_path.exists():
        try:
            return json.loads(weights_path.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def get_precedents(proposal: str, k: int = 5):
    """
    Uses scripts/get_precedents.py which you already have working.
    Returns list[dict] like: [{rank, score, meta:{id,text_fields...}}, ...]
    """
    try:
        cmd = ["python3", "scripts/get_precedents.py", proposal]
        raw = subprocess.check_output(cmd, text=True).strip()
        if not raw:
            return []
        data = json.loads(raw)
        if isinstance(data, list):
            return data[:k]
        return []
    except Exception:
        return []

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("payload_json", help="Path to payload JSON")
    ap.add_argument("-o", "--out", default="logs/report_latest_case_officer.md", help="Output markdown path")
    ap.add_argument("--include-precedents", action="store_true", help="Include similar applications section")
    args = ap.parse_args()

    payload_path = Path(args.payload_json)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    data = safe_json_load(payload_path)

    proposal = data.get("input", {}).get("proposal_text", "[AUTO]")
    authority = data.get("input", {}).get("authority", "newcastle")
    doc_keys = data.get("input", {}).get("doc_keys", [])
    signals = data.get("signals", {}) or {}
    policy = data.get("policy", {}) or {}
    report = data.get("report", {}) or {}

    citations = policy.get("citations", []) or []
    evidence = policy.get("evidence", []) or []

    # Rerank and enforce doc diversity (so NPPF + DAP can surface)
    weights = load_weights()
    if rerank_policy is not None and (citations or evidence):
        try:
            citations, evidence = rerank_policy(citations, evidence, weights)
        except Exception:
            pass

    decision = report.get("decision", "needs_officer_review")
    confidence = report.get("confidence", "")
    summary = report.get("summary", "")
    conditions = report.get("draft_conditions", []) or []

    today = datetime.now().strftime("%Y-%m-%d")

    lines = []
    lines.append("# CASE OFFICER REPORT (DRAFT – PILOT, DETERMINISTIC)\n")
    lines.append(f"**Authority:** Newcastle City Council  \n")
    lines.append(f"**Application Ref:** [OFFICER REVIEW]  \n")
    lines.append(f"**Site Address:** [OFFICER REVIEW]  \n")
    lines.append(f"**Proposal:** {proposal}  \n")
    lines.append(f"**Date:** {today}  \n")
    if confidence != "":
        lines.append(f"**Engine Confidence (heuristic):** {confidence}  \n")
    lines.append("\n---\n")

    # 1) Site
    lines.append("## 1.0 Site and Surroundings\n")
    lines.append("[OFFICER REVIEW] Describe site context, planning history, constraints (conservation area/listed/flood/highways), and surrounding uses.\n")

    # 2) Proposal
    lines.append("\n## 2.0 The Proposal\n")
    lines.append(f"{proposal}\n")

    # 3) Policy
    lines.append("\n## 3.0 Relevant Planning Policy\n")
    lines.append("### 3.1 Documents Queried (pilot)\n")
    lines.append(f"- Authority: `{authority}`\n")
    if doc_keys:
        lines.append("- Documents: " + ", ".join(doc_keys) + "\n")

    lines.append("\n### 3.2 Key Policy Evidence (ranked + filtered)\n")
    if citations:
        for c in citations[:10]:
            lines.append(
                f"- **{c.get('doc_title','')}** ({c.get('doc_key','')}) — {c.get('paragraph_ref','')} "
                f"(pp.{c.get('page_start','')}-{c.get('page_end','')}) — score {c.get('score','')}\n"
            )
    else:
        lines.append("- None returned.\n")

    lines.append("\n### 3.3 Evidence Excerpts (traceability)\n")
    if evidence:
        for e in evidence[:6]:
            lines.append(f"**{e.get('doc_title','')}** — {e.get('paragraph_ref','')} (score {e.get('score','')})\n")
            snippet = (e.get("snippet") or e.get("text") or "").strip()
            if snippet:
                block = "\n".join(snippet.splitlines()[:10])
                lines.append("> " + "\n> ".join(block.splitlines()) + "\n")
            lines.append(f"*(Source: {e.get('source_path','')}; pages {e.get('page_start','')}-{e.get('page_end','')})*\n\n")
    else:
        lines.append("- None returned.\n")

    # 3.4 Precedents
    lines.append("### 3.4 Similar applications (precedents)\n")
    lines.append("> Retrieved from Newcastle planning portal dataset via FAISS similarity search. Use for context/checks, not as definitive constraints.\n\n")

    precedents = get_precedents(proposal, k=5) if args.include_precedents else []
    if precedents:
        for pr in precedents:
            meta = pr.get("meta") or {}
            ref = meta.get("reference") or meta.get("app_ref") or meta.get("application_ref") or meta.get("uid") or meta.get("id") or "[id]"
            addr = meta.get("address") or meta.get("site") or meta.get("site_address") or meta.get("location") or ""
            tf = meta.get("text_fields") or []
            title = tf[0] if isinstance(tf, list) and tf else ""
            lines.append(f"- **{ref}** — {addr}  \n")
            if title:
                lines.append(f"  - Proposal: {title}\n")
            lines.append(f"  - Similarity score: {pr.get('score')}\n")
    else:
        lines.append("- None returned (or --include-precedents not enabled).\n")

    # 4) Consultation placeholder
    lines.append("\n## 4.0 Consultation Responses\n")
    lines.append("[OFFICER REVIEW] Summarise internal/external consultation responses (highways, environmental health, neighbours, etc.).\n")

    # 5) Assessment
    lines.append("\n## 5.0 Assessment\n")
    lines.append("### 5.1 Principle of Development\n")
    lines.append("- The proposal is assessed against the Development Plan and the NPPF, having regard to the policy evidence cited above.\n\n")

    lines.append("### 5.2 Residential Amenity\n")
    lines.append("- [OFFICER REVIEW] Consider privacy, overlooking, noise, outlook, and internal living conditions.\n\n")

    lines.append("### 5.3 Highway Safety / Parking / Servicing\n")
    lines.append("- [OFFICER REVIEW] Confirm refuse storage/collection, cycle storage, parking stress and servicing arrangements.\n\n")

    lines.append("### 5.4 Design and Character\n")
    lines.append("- [OFFICER REVIEW] Confirm whether any external alterations affect character/appearance, including heritage impacts if relevant.\n\n")

    lines.append("### 5.5 Climate / Sustainability\n")
    lines.append("- Sustainability/energy efficiency is a material consideration; apply conditions/notes where relevant.\n")

    # Signals (useful)
    lines.append("\n### 5.6 Key signals from the pilot run\n")
    if signals:
        lines.append(f"- Policy matches returned: **{signals.get('policy_count','')}**\n")
        lines.append(f"- Average policy score: **{signals.get('policy_avg_score','')}**\n")
    else:
        lines.append("- No signals returned.\n")

    # 6) Planning balance
    lines.append("\n## 6.0 Planning Balance\n")
    lines.append("- The planning balance should weigh the proposal’s benefits against any harms and uncertainties.\n")
    if summary:
        lines.append(f"- Engine summary: {summary}\n")

    # 7) Recommendation
    lines.append("\n## 7.0 Recommendation\n")
    lines.append(f"**Recommendation (pilot):** `{decision}`\n")

    if "approve" in str(decision).lower():
        lines.append("\n### 7.1 Conditions (Draft – Officer Review)\n")
        if conditions:
            for i, cond in enumerate(conditions, 1):
                lines.append(f"{i}. {cond}\n")
        else:
            lines.append("1. Time limit (3 years).\n")
            lines.append("2. Approved plans.\n")

    elif "refuse" in str(decision).lower():
        lines.append("\n### 7.1 Reasons for Refusal (Draft – Officer Review)\n")
        lines.append("- [AUTO] Insert reasons linked to policy conflicts evidenced above.\n")

    # 8) Appendix
    lines.append("\n## 8.0 Evidence Appendix (Traceability)\n")
    lines.append(f"- Payload source: `{payload_path}`\n")
    lines.append(f"- Weights config: `{Path('config/relevance_weights.json').resolve()}`\n")

    out_path.write_text("".join(lines), encoding="utf-8")
    print(f"OK: wrote {out_path}")

if __name__ == "__main__":
    main()
