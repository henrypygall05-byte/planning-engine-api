from typing import Dict, Any, List

def require_policy_evidence(retrieval_out: Dict[str, Any], min_results: int = 3) -> Dict[str, Any]:
    """
    Enforces: no answer without citations.
    Returns a standard structure your report generator can consume.
    """
    ok = retrieval_out.get("ok", False)
    results: List[Dict[str, Any]] = retrieval_out.get("results") or []

    if (not ok) or (len(results) < min_results):
        return {
            "ok": False,
            "reason": retrieval_out.get("reason") or "Insufficient policy evidence",
            "citations": [],
            "evidence": []
        }

    citations = []
    for r in results:
        citations.append({
            "authority": r.get("authority"),
            "doc_key": r.get("doc_key"),
            "doc_title": r.get("doc_title"),
            "paragraph_ref": r.get("paragraph_ref"),
            "page_start": r.get("page_start"),
            "page_end": r.get("page_end"),
            "source_path": r.get("source_path"),
            "score": r.get("score"),
        })

    return {
        "ok": True,
        "reason": None,
        "citations": citations,
        "evidence": results
    }
