"""Write-time memory scoring shared by extraction and retrieval."""

from __future__ import annotations

import re
from dataclasses import dataclass


SOURCE_PRIORS = {
    "vadim-said": 1.0,
    "signal": 0.9,
    "tg": 0.9,
    "telegram": 0.9,
    "whatsapp": 0.9,
    "imessage": 0.9,
    "gmail": 0.9,
    "gcal": 0.9,
    "gh": 0.9,
    "gtasks": 0.9,
    "gdrive": 0.85,
    "hlopya": 0.85,
    "bee": 0.75,
    "obsidian": 0.75,
    "dayflow": 0.5,
    "browser": 0.45,
    "xnews": 0.35,
}


@dataclass(frozen=True)
class MemoryScore:
    importance: int
    confidence: int
    durability: int
    source_prior: float
    score: float
    route: str


def source_prior(source_uri: str, claim_scope: str = "speaker") -> float:
    scheme = source_uri.split(":", 1)[0].lower()
    if scheme == "vadimgest" and "://" in source_uri:
        scheme = source_uri.split("://", 1)[1].split("/", 1)[0].lower()
    prior = SOURCE_PRIORS.get(scheme, 0.6)
    if claim_scope == "external" and scheme in {
        "signal",
        "tg",
        "telegram",
        "whatsapp",
        "imessage",
        "gmail",
        "hlopya",
        "bee",
    }:
        prior *= 0.85
    return round(prior, 3)


def score_memory(
    *,
    importance: int,
    confidence: int,
    durability: int,
    source_uri: str,
    claim_scope: str = "speaker",
    hard_keep: bool = False,
) -> MemoryScore:
    for name, value in {
        "importance": importance,
        "confidence": confidence,
        "durability": durability,
    }.items():
        if not 1 <= value <= 10:
            raise ValueError(f"{name} must be between 1 and 10")

    prior = source_prior(source_uri, claim_scope=claim_scope)
    score = round(
        prior * (0.45 * importance + 0.30 * durability + 0.25 * confidence),
        1,
    )

    if confidence >= 7 and (hard_keep or score >= 6.0):
        route = "state_log"
    elif confidence >= 5 and (hard_keep or score >= 3.5):
        route = "log_only"
    else:
        route = "lake"

    return MemoryScore(
        importance=importance,
        confidence=confidence,
        durability=durability,
        source_prior=prior,
        score=score,
        route=route,
    )


_SCORE_PATTERNS = (
    re.compile(r"·\s*score:\s*`?([0-9]+(?:\.[0-9]+)?)", re.IGNORECASE),
    re.compile(
        r"\*\*memory-score:\*\*\s*`?([0-9]+(?:\.[0-9]+)?)",
        re.IGNORECASE,
    ),
    re.compile(r"\bscore=([0-9]+(?:\.[0-9]+)?)", re.IGNORECASE),
)


def extract_document_memory_score(content: str, default: float = 5.0) -> float:
    scores = [
        float(match.group(1))
        for pattern in _SCORE_PATTERNS
        for match in pattern.finditer(content)
    ]
    return max(scores, default=default)


def memory_boost(score: float) -> float:
    bounded = min(10.0, max(0.0, score))
    return 0.85 + 0.03 * bounded
