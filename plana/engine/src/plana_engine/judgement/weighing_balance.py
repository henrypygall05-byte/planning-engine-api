from __future__ import annotations

from typing import Any, Dict, List


def _topic_from_text(q: str) -> str:
    ql = (q or "").lower()
    if any(k in ql for k in ["rear extension", "single storey", "two storey", "loft", "dormer", "outbuilding", "garage", "porch"]):
        return "householder"
    if any(k in ql for k in ["listed", "conservation area", "heritage", "historic"]):
        return "heritage"
    if any(k in ql for k in ["parking", "traffic", "highway", "visibility", "access", "junction"]):
        return "highways"
    if any(k in ql for k in ["flood", "surface water", "drainage", "suds"]):
        return "flood"
    return "general"


def weigh_balance(proposal_text: str, policy_block: Dict[str, Any]) -> Dict[str, Any]:
    """
    Deterministic weighing-balance v1 (NO LLM):
    - Uses retrieved policy chunks to infer likely issues + a provisional decision.
    - Always returns a structured object the report generator can consume.
    """
    results: List[Dict[str, Any]] = (policy_block or {}).get("results") or []
    if not results:
        return {
            "decision": "insufficient_evidence",
            "confidence": 0.0,
            "reasons_for": [],
            "reasons_against": ["No policy evidence retrieved."],
            "suggested_conditions": [],
            "issues": [],
        }

    # Basic scoring signals
    scores = [float(r.get("score") or 0.0) for r in results]
    avg_score = sum(scores) / max(len(scores), 1)
    top_score = max(scores) if scores else 0.0

    topic = _topic_from_text(proposal_text)

    # Extract issues from evidence text
    text_blob = "\n".join((r.get("text") or "")[:4000].lower() for r in results)
    issues = []
    if topic == "householder":
        if any(k in text_blob for k in ["amenity", "privacy", "overlooking", "daylight", "sunlight", "outlook"]):
            issues.append("Neighbour amenity (privacy/daylight/outlook)")
        if any(k in text_blob for k in ["design", "scale", "materials", "character"]):
            issues.append("Design/scale/materials/character")
    if topic == "heritage":
        issues.append("Heritage significance/setting")
    if topic == "highways":
        issues.append("Highway safety/parking/access")
    if topic == "flood":
        issues.append("Flood risk/surface water drainage")

    # Decision rules (v1)
    # Keep it conservative: only "approve_with_conditions" if evidence is strong.
    decision = "needs_officer_review"
    confidence = 0.45

    if avg_score >= 4.0 and top_score >= 4.5:
        decision = "approve_with_conditions"
        confidence = 0.70
    elif avg_score < 2.5:
        decision = "insufficient_evidence"
        confidence = 0.25

    # Reasons (v1)
    reasons_for = []
    reasons_against = []

    if decision == "approve_with_conditions":
        reasons_for.append("Policy evidence indicates the proposal can be acceptable subject to detailed design/amenity safeguards.")
        if "Neighbour amenity (privacy/daylight/outlook)" in issues:
            reasons_for.append("Key amenity considerations are addressed through assessment against amenity-focused policies.")
        if "Design/scale/materials/character" in issues:
            reasons_for.append("Design/scale/materials can be controlled through conditions and adherence to approved plans.")
    elif decision == "needs_officer_review":
        reasons_against.append("Policy evidence retrieved but requires officer judgement to weigh impacts and site context.")
        if issues:
            reasons_against.append(f"Main issues flagged: {', '.join(issues)}.")
    else:
        reasons_against.append("Retrieval quality appears weak for drawing a conclusion; expand documents / improve retrieval filtering.")

    # Conditions (v1) — generic, safe
    suggested_conditions = []
    if decision == "approve_with_conditions":
        suggested_conditions = [
            "Works to be carried out in accordance with the approved plans.",
            "External materials to match existing dwelling unless otherwise agreed in writing by the LPA.",
        ]
        if "Neighbour amenity (privacy/daylight/outlook)" in issues:
            suggested_conditions.append("No additional windows/openings facing neighbouring properties unless otherwise agreed in writing by the LPA.")

    return {
        "decision": decision,
        "confidence": float(confidence),
        "reasons_for": reasons_for,
        "reasons_against": reasons_against,
        "suggested_conditions": suggested_conditions,
        "issues": issues,
    }
