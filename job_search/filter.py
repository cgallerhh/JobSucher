"""
Relevance scoring for job listings based on Christian Galler's profile.

Score 0–100:
  ≥ 70  → Sehr hoch (grün)
  ≥ 50  → Hoch (hellgrün)
  ≥ 35  → Mittel (gelb)
  ≥ MIN_SCORE → Relevant (blau)
"""
from .config import MIN_SCORE, NEGATIVE_KEYWORDS, POSITIVE_KEYWORDS


def score_job(job: dict) -> int:
    """Return a relevance score 0–100 for a job dict."""
    text = " ".join(
        [
            job.get("title", ""),
            job.get("description", ""),
            job.get("company", ""),
            job.get("location", ""),
        ]
    ).lower()

    score = 0

    for keyword, points in POSITIVE_KEYWORDS.items():
        if keyword.lower() in text:
            score += points

    for keyword, penalty in NEGATIVE_KEYWORDS.items():
        if keyword.lower() in text:
            score += penalty  # penalty is already negative

    return max(0, min(100, score))


def is_relevant(score: int) -> bool:
    return score >= MIN_SCORE
