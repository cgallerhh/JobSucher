"""
AI-powered job relevance scorer using Claude API (Haiku = cost-efficient).

Falls back silently to keyword scores when:
  - ANTHROPIC_API_KEY is not set
  - anthropic package is not installed
  - API call fails for a specific job

Cost estimate: ~0.05 €/day for 100 jobs (claude-haiku-4-5-20251001)
"""
import json
import logging
import os
from pathlib import Path
from typing import Dict, List

logger = logging.getLogger(__name__)

CONTEXT_DIR = Path("context")
MODEL = "claude-haiku-4-5-20251001"


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


def score_jobs_with_ai(jobs: List[Dict]) -> List[Dict]:
    """
    Re-score jobs using Claude API.
    Returns the same list with updated 'score' and new 'ai_reason' fields.
    Jobs where AI scoring fails keep their original keyword score.
    """
    try:
        import anthropic
    except ImportError:
        logger.info("anthropic package not installed – keeping keyword scores")
        return jobs

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        logger.info("ANTHROPIC_API_KEY not set – keeping keyword scores")
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
    client = anthropic.Anthropic(api_key=api_key)

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

            response = client.messages.create(
                model=MODEL,
                max_tokens=120,
                system=system,
                messages=[{"role": "user", "content": job_text}],
            )

            raw = response.content[0].text.strip() if response.content else ""
            if not raw:
                raise ValueError("Empty response from model")
            # Strip markdown code fences if model wraps JSON in ```json ... ```
            if raw.startswith("```"):
                raw = raw.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
            result = json.loads(raw)
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
