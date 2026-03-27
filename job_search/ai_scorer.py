"""
AI-powered job relevance scorer using Claude API (Haiku = cost-efficient).

Uses the Anthropic REST API directly via `requests` (no SDK dependency).

Falls back silently to keyword scores when:
  - ANTHROPIC_API_KEY is not set
  - API call fails for a specific job

Cost estimate: ~0.05 €/day for 100 jobs (claude-haiku-4-5-20251001)
"""
import json
import logging
import os
from pathlib import Path
from typing import Dict, List

import requests

logger = logging.getLogger(__name__)

CONTEXT_DIR = Path("context")
MODEL = "claude-haiku-4-5-20251001"
API_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_VERSION = "2023-06-01"


def _load_context() -> str:
    """Concatenate all .md files in context/ (except README) into one string."""
    if not CONTEXT_DIR.exists():
        return ""
    parts = []
    for md_file in sorted(CONTEXT_DIR.glob("*.md")):
        if md_file.name.lower() == "readme.md":
            continue
        text = md_file.read_text(encoding="utf-8").strip()
        if text:
            heading = md_file.stem.replace("_", " ").title()
            parts.append(f"## {heading}\n\n{text}")
    return "\n\n---\n\n".join(parts)


def _system_prompt(context: str) -> str:
    return f"""Du bist ein spezialisierter Karriere-Assistent für einen Senior Sales Manager \
im GKV- und Public-Sector-IT-Markt. Bewerte eingehende Stellenanzeigen auf Relevanz.

{context}

---

BEWERTUNGSSCHEMA (score 0–100):
• 80–100 — Perfekter Match: Sales/Account-Rolle + GKV oder Public Sector IT + Senior-Level
• 60–79  — Sehr gut: 2 von 3 Kernkriterien erfüllt, klar verwandtes Umfeld
• 40–59  — Teilweise: IT-Consulting oder Gesundheitswesen ohne direkten GKV-Vertriebsfokus
• 25–39  — Grenzwertig: entfernt relevant, könnte trotzdem einen Blick wert sein
• 0–24   — Nicht relevant: falsche Branche, falsches Level oder kein Vertriebsbezug

Antworte AUSSCHLIESSLICH mit minimalem JSON (kein Markdown, kein Kommentar):
{{"score": <int 0-100>, "reason": "<max 90 Zeichen auf Deutsch>"}}"""


def _call_api(api_key: str, system: str, job_text: str) -> dict:
    """Make a single API call to Anthropic REST endpoint. Returns parsed JSON."""
    response = requests.post(
        API_URL,
        headers={
            "x-api-key": api_key,
            "anthropic-version": ANTHROPIC_VERSION,
            "content-type": "application/json",
        },
        json={
            "model": MODEL,
            "max_tokens": 120,
            "system": system,
            "messages": [{"role": "user", "content": job_text}],
        },
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()
    raw = data["content"][0]["text"].strip()
    if not raw:
        raise ValueError("Empty response from model")
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
    return json.loads(raw)


def score_jobs_with_ai(jobs: List[Dict]) -> List[Dict]:
    """
    Re-score jobs using Claude API.
    Returns the same list with updated 'score' and new 'ai_reason' fields.
    Jobs where AI scoring fails keep their original keyword score.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        logger.warning("ANTHROPIC_API_KEY not set – keeping keyword scores")
        return jobs
    if not api_key.isascii():
        non_ascii = [(i, c, f"U+{ord(c):04X}") for i, c in enumerate(api_key) if ord(c) > 127]
        logger.error(
            "ANTHROPIC_API_KEY contains non-ASCII character(s) at position(s) %s "
            "– likely a look-alike character (e.g., Cyrillic К instead of Latin K). "
            "Re-copy the key from https://console.anthropic.com/settings/keys",
            ", ".join(f"{i} ({cp})" for i, _, cp in non_ascii),
        )
        return jobs

    context = _load_context()
    if not context:
        logger.warning("context/ folder is empty – AI scoring will have less context")

    system = _system_prompt(context)

    # Connectivity check: one quick test call before scoring all jobs
    try:
        _call_api(api_key, "Reply with just: OK", "test")
    except Exception as exc:
        logger.error("Anthropic API not reachable (connection check failed): %s – keeping keyword scores for all jobs", exc)
        return jobs

    scored: List[Dict] = []
    for job in jobs:
        try:
            job_text = (
                f"Jobtitel: {job.get('title', '')}\n"
                f"Unternehmen: {job.get('company', '')}\n"
                f"Standort: {job.get('location', '')}\n"
                f"Stellenbeschreibung (Auszug): {job.get('description', '')[:800]}\n"
                f"Quelle: {job.get('source', '')}"
            )

            result = _call_api(api_key, system, job_text)
            ai_score = max(0, min(100, int(result.get("score", 0))))
            ai_reason = result.get("reason", "")

            scored.append(
                {
                    **job,
                    "score": ai_score,
                    "ai_reason": ai_reason,
                }
            )
            logger.debug(
                "AI score %d/100 for '%s' – %s", ai_score, job.get("title"), ai_reason
            )

        except Exception as exc:
            logger.warning(
                "AI scoring failed for '%s': %s – keeping keyword score",
                job.get("title"),
                exc,
            )
            scored.append(job)

    ai_scored_count = sum(1 for j in scored if "ai_reason" in j)
    logger.info("AI scored %d/%d jobs", ai_scored_count, len(jobs))
    return scored
