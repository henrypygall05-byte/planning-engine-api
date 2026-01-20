import re
from typing import Any, Dict, List, Tuple

def _lower(s: str) -> str:
    return (s or "").lower()

def load_weights(cfg: Dict[str, Any]) -> Dict[str, Any]:
    # cfg is already loaded JSON from config/relevance_weights.json
    cfg = cfg or {}
    cfg.setdefault("doc_diversity_target", 3)
    cfg.setdefault("max_evidence_items", 10)
    cfg.setdefault("c3_keyword_boost", 2.0)
    cfg.setdefault("c3_keywords", [])
    cfg.setdefault("irrelevance_penalty_per_hit", 0.7)
    cfg.setdefault("min_score_floor", 0.1)
    cfg.setdefault("doc_boost", {})
    cfg.setdefault("topic_penalties", {})
    return cfg

def score_item(text: str, base_score: float, doc_key: str, w: Dict[str, Any]) -> float:
    t = _lower(text)
    score = float(base_score or 0.0)

    # Doc boosts (encourage NPPF + DAP to surface)
    score *= float(w.get("doc_boost", {}).get(doc_key, 1.0))

    # C3 relevance boosts
    boost = float(w.get("c3_keyword_boost", 1.0))
    keywords = w.get("c3_keywords", []) or []
    hits = 0
    for kw in keywords:
        if kw and _lower(kw) in t:
            hits += 1
    if hits:
        score *= (1.0 + (boost - 1.0) * min(1.0, hits / 3.0))

    # Topic penalties (downrank leisure/tourism/retail/etc)
    pen_per_hit = float(w.get("irrelevance_penalty_per_hit", 0.7))
    topic_penalties = w.get("topic_penalties", {}) or {}
    penalty_hits = 0
    for topic, mult in topic_penalties.items():
        if topic and _lower(topic) in t:
            penalty_hits += 1
            score *= (1.0 / float(mult or 1.0))
    if penalty_hits:
        score -= penalty_hits * pen_per_hit

    # Floor
    floor = float(w.get("min_score_floor", 0.1))
    if score < floor:
        score = floor
    return float(score)

def rerank_policy(
    citations: List[Dict[str, Any]],
    evidence: List[Dict[str, Any]],
    weights: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Returns (new_citations, new_evidence) with:
    - reranked scores
    - doc diversity enforced
    """
    w = load_weights(weights)

    # Attach computed score
    scored = []
    for e in (evidence or []):
        doc_key = e.get("doc_key") or ""
        text = (e.get("snippet") or e.get("text") or "")
        base = e.get("score") or 0.0
        e2 = dict(e)
        e2["_rerank_score"] = score_item(text, float(base), str(doc_key), w)
        scored.append(e2)

    scored.sort(key=lambda x: float(x.get("_rerank_score", 0.0)), reverse=True)

    # Enforce doc diversity
    target_docs = int(w.get("doc_diversity_target", 3))
    max_items = int(w.get("max_evidence_items", 10))

    picked: List[Dict[str, Any]] = []
    seen_docs = set()

    # Pass 1: pick best per doc until we hit doc diversity target
    for e in scored:
        dk = e.get("doc_key")
        if dk and dk not in seen_docs:
            picked.append(e)
            seen_docs.add(dk)
            if len(seen_docs) >= target_docs:
                break

    # Pass 2: fill remaining slots by score
    for e in scored:
        if len(picked) >= max_items:
            break
        if e in picked:
            continue
        picked.append(e)

    # Build citations list from picked evidence (keep traceability)
    # Prefer citation objects if present, else synthesize from evidence
    cit_map = {}
    for c in (citations or []):
        key = (c.get("doc_key"), c.get("paragraph_ref"))
        cit_map[key] = c

    new_cits = []
    for e in picked:
        key = (e.get("doc_key"), e.get("paragraph_ref"))
        if key in cit_map:
            new_cits.append(cit_map[key])
        else:
            new_cits.append({
                "authority": e.get("authority"),
                "doc_key": e.get("doc_key"),
                "doc_title": e.get("doc_title"),
                "paragraph_ref": e.get("paragraph_ref"),
                "page_start": e.get("page_start"),
                "page_end": e.get("page_end"),
                "source_path": e.get("source_path"),
                "score": e.get("_rerank_score"),
            })

    return new_cits, picked
