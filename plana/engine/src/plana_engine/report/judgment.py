from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
import re

AMENITY_WORDS = ["residential amenity", "privacy", "overlooking", "daylight", "sunlight", "outlook", "noise", "disturbance"]
DESIGN_WORDS = ["design", "character", "scale", "massing", "materials", "appearance"]
HERITAGE_WORDS = ["heritage", "listed", "conservation area", "setting", "significance"]
HIGHWAYS_WORDS = ["highway", "parking", "traffic", "access", "visibility", "junction"]
FLOOD_WORDS = ["flood", "drainage", "surface water", "suds"]
TREES_WORDS = ["tree", "tpo", "arboricultural", "hedgerow"]

def detect_issues(proposal_text: str) -> List[str]:
    t = (proposal_text or "").lower()
    issues = []
    if any(w in t for w in ["rear extension", "single storey", "two storey", "loft", "dormer", "outbuilding", "porch"]):
        issues.append("householder")
    if any(w in t for w in AMENITY_WORDS):
        issues.append("amenity")
    if any(w in t for w in DESIGN_WORDS):
        issues.append("design")
    if any(w in t for w in HERITAGE_WORDS):
        issues.append("heritage")
    if any(w in t for w in HIGHWAYS_WORDS):
        issues.append("highways")
    if any(w in t for w in FLOOD_WORDS):
        issues.append("flood")
    if any(w in t for w in TREES_WORDS):
        issues.append("trees")
    if not issues:
        issues = ["general"]
    # Deduplicate preserving order
    seen = set()
    out = []
    for x in issues:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out

def _avg_top_scores(evidence: List[Dict[str, Any]], n: int = 6) -> float:
    if not evidence:
        return 0.0
    scores = [float(e.get("score") or 0.0) for e in evidence]
    scores.sort(reverse=True)
    top = scores[:n]
    return sum(top) / max(1, len(top))

def _policy_strength(policy_block: Dict[str, Any]) -> Tuple[bool, float, str]:
    if not policy_block or not policy_block.get("ok"):
        return False, 0.0, "no_policy_results"
    ev = policy_block.get("evidence") or policy_block.get("results") or []
    avg = _avg_top_scores(ev, n=6)
    if len(ev) < 3:
        return False, avg, "policy_results_too_few"
    if avg < 3.0:
        return False, avg, "policy_scores_too_low"
    return True, avg, "ok"

def _case_strength(case_block: Optional[Dict[str, Any]]) -> Tuple[bool, float, str]:
    if not case_block:
        # Precedents not wired yet; treat as optional by returning ok=False but reason.
        return False, 0.0, "precedents_not_enabled"
    if not case_block.get("ok"):
        return False, 0.0, case_block.get("reason") or "no_case_results"
    ev = case_block.get("results") or []
    avg = _avg_top_scores(ev, n=5)
    if len(ev) < 1:
        return False, avg, "case_results_too_few"
    if avg < 2.0:
        return False, avg, "case_scores_too_low"
    return True, avg, "ok"

def _draft_householder_conditions(policy_evidence: List[Dict[str, Any]], proposal_text: str) -> List[str]:
    # Keep it conservative; you can enrich later with policy-triggered condition templates.
    conds = []
    conds.append("The development hereby approved shall be carried out in accordance with the approved plans and supporting documents.")
    if any(k in (proposal_text or "").lower() for k in ["materials", "match existing"]):
        conds.append("External materials shall match the existing dwelling unless otherwise agreed in writing by the Local Planning Authority.")
    if any("privacy" in (proposal_text or "").lower() or "overlooking" in (proposal_text or "").lower() for _ in [0]):
        conds.append("Any side-facing windows serving habitable rooms shall be obscure glazed and non-opening below 1.7m from finished floor level.")
    return conds

def make_recommendation(
    proposal_text: str,
    policy_block: Dict[str, Any],
    case_block: Optional[Dict[str, Any]] = None,
    require_precedent: bool = False
) -> Dict[str, Any]:
    issues = detect_issues(proposal_text)

    policy_ok, policy_score, policy_reason = _policy_strength(policy_block)
    case_ok, case_score, case_reason = _case_strength(case_block)

    # Hard stop rules
    if not policy_ok:
        return {
            "decision": "insufficient_evidence",
            "reason": f"policy_retrieval_weak:{policy_reason}",
            "issues": issues,
            "signals": {"policy_avg_score": policy_score, "case_avg_score": case_score, "case_reason": case_reason},
            "draft_conditions": [],
            "draft_refusal_reasons": []
        }

    if require_precedent and not case_ok:
        return {
            "decision": "insufficient_evidence",
            "reason": f"precedent_retrieval_weak:{case_reason}",
            "issues": issues,
            "signals": {"policy_avg_score": policy_score, "case_avg_score": case_score, "case_reason": case_reason},
            "draft_conditions": [],
            "draft_refusal_reasons": []
        }

    # Deterministic “planning balance” starter:
    # - If proposal explicitly mentions harm (loss of light/overlooking) we treat as higher risk → default to approve_with_conditions unless evidence shows refusal.
    t = (proposal_text or "").lower()
    harm_flags = any(k in t for k in ["loss of light", "overlooking", "overbearing", "unacceptable"])

    # If precedents are enabled, use them to tilt:
    tilt_refuse = False
    tilt_conditions = True

    if case_ok:
        for c in (case_block.get("results") or [])[:5]:
            dec = (c.get("decision") or "").lower()
            reasons = (c.get("reasons_text") or c.get("officer_report_text") or "").lower()
            if "refus" in dec and any(k in reasons for k in ["loss of daylight", "overbearing", "privacy", "overlooking"]):
                tilt_refuse = True
            if "approved" in dec and any(k in (c.get("conditions_text") or "").lower() for k in ["materials", "obscure", "glazing", "plans"]):
                tilt_conditions = True

    if tilt_refuse and harm_flags:
        refusal = [
            "The proposal would result in unacceptable harm to the residential amenity of neighbouring occupiers by reason of loss of light / overbearing impact / loss of privacy, contrary to the relevant Development Plan policies."
        ]
        return {
            "decision": "refuse",
            "reason": None,
            "issues": issues,
            "signals": {"policy_avg_score": policy_score, "case_avg_score": case_score, "case_reason": case_reason},
            "draft_conditions": [],
            "draft_refusal_reasons": refusal
        }

    # Default to approve with conditions for householder-type where policy evidence is strong
    conds = _draft_householder_conditions(policy_block.get("evidence") or [], proposal_text) if tilt_conditions else []
    decision = "approve_with_conditions" if conds else "approve"

    return {
        "decision": decision,
        "reason": None,
        "issues": issues,
        "signals": {"policy_avg_score": policy_score, "case_avg_score": case_score, "case_reason": case_reason},
        "draft_conditions": conds,
        "draft_refusal_reasons": []
    }
