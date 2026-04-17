"""
AI-powered job relevance scorer using OpenAI API (gpt-4o-mini = cost-efficient).

Verwendet die OpenAI REST API direkt via `requests` (kein SDK erforderlich).

Optimierungen:
  - OpenAI Auto-Caching (>1024 Token): System-Prompt gecacht → 50 % Token-Rabatt automatisch
  - Parallele API-Calls (ThreadPoolExecutor, 10 Workers) → ~10× schneller als sequenziell
  - Ausführliche Bewertung: score + reason + strengths + concerns + action
  - 1 000 Zeichen Beschreibungstext (Kerninformationen stehen immer am Anfang)
  - Connectivity-Check vor Massen-Scoring (schnelles Fail-Fast)

Fallback: Bei fehlendem API-Key oder API-Fehler bleiben Keyword-Scores unverändert.
Cost estimate: ~0.02 €/day for 100 jobs (gpt-4o-mini + auto prompt caching)
"""
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests

logger = logging.getLogger(__name__)

CONTEXT_DIR = Path("context")
MODEL = "gpt-4o-mini"
API_URL = "https://api.openai.com/v1/chat/completions"
MAX_WORKERS = 10       # OpenAI hat höhere Rate Limits als Claude Haiku
MAX_DESC_CHARS = 1000  # Kerninformationen stecken in den ersten 1000 Zeichen


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
im GKV- und Public-Sector-IT-Markt. Bewerte Stellenanzeigen ausführlich und präzise.

{context}

---

SCHEMA score 0-100: 80-100=Perfekter Match(Sales+GKV+Senior), 60-79=Sehr gut(2/3 Kernkriterien), \
40-59=Teilweise(IT/GW ohne GKV-Vertrieb), 25-39=Grenzwertig, 0-24=Nicht relevant.
AUSSCHLUSS score=0: adesso SE, HBSN Consulting, Init AG, AOK-Verbund.

Antworte NUR mit minimalem JSON (kein Markdown):
{{"score":<int 0-100>,"reason":"<max 120 Zeichen>","strengths":["<1-3 Treffer>"],"concerns":["<0-2 Bedenken>"],"action":"<Sofort bewerben|Pruefen|Ueberspringen>"}}
action: >=70→Sofort bewerben, 40-69→Pruefen, <40→Ueberspringen. concerns=[] wenn keine."""


def _headers(api_key: str) -> dict:
    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }


def _call_api(api_key: str, system_content: list, job_text: str) -> dict:
    """Single API call mit gecachtem System-Prompt."""
    response = requests.post(
        API_URL,
        headers=_headers(api_key),
        json={
            "model": MODEL,
            "max_tokens": 300,
            "messages": system_content + [{"role": "user", "content": job_text}],
        },
        timeout=30,
    )
    response.raise_for_status()
    raw = response.json()["choices"][0]["message"]["content"].strip()
    if not raw:
        raise ValueError("Empty response from model")
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
    return json.loads(raw)


def _score_single(
    api_key: str,
    system_content: list,
    job: Dict,
) -> Tuple[Dict, Optional[Exception]]:
    """Score a single job. Returns (updated_job, error_or_None)."""
    try:
        job_text = (
            f"Jobtitel: {job.get('title', '')}\n"
            f"Unternehmen: {job.get('company', '')}\n"
            f"Standort: {job.get('location', '')}\n"
            f"Stellenbeschreibung: {job.get('description', '')[:MAX_DESC_CHARS]}\n"
            f"Quelle: {job.get('source', '')}"
        )
        result = _call_api(api_key, system_content, job_text)
        ai_score = max(0, min(100, int(result.get("score", 0))))
        return {
            **job,
            "score": ai_score,
            "ai_reason": result.get("reason", ""),
            "ai_strengths": result.get("strengths", []),
            "ai_concerns": result.get("concerns", []),
            "ai_action": result.get("action", ""),
        }, None
    except Exception as exc:
        return job, exc


def score_jobs_with_ai(jobs: List[Dict]) -> List[Dict]:
    """
    Re-score jobs using OpenAI API (parallel + auto prompt caching).
    Returns the same list with updated 'score' and new AI fields.
    Jobs where AI scoring fails keep their original keyword score.
    """
    api_key = os.environ.get("OPENAI_API_KEY", "")
    if not api_key:
        logger.warning("OPENAI_API_KEY not set - keeping keyword scores")
        return jobs
    if not api_key.isascii():
        non_ascii = [(i, c, f"U+{ord(c):04X}") for i, c in enumerate(api_key) if ord(c) > 127]
        logger.error(
            "OPENAI_API_KEY contains non-ASCII character(s) at position(s) %s "
            "- likely a look-alike character (e.g., Cyrillic K instead of Latin K). "
            "Re-copy the key from https://platform.openai.com/api-keys",
            ", ".join(f"{i} ({cp})" for i, _, cp in non_ascii),
        )
        return jobs

    context = _load_context()
    if not context:
        logger.warning("context/ folder is empty - AI scoring will have less context")

    # Connectivity check: schnelles Fail-Fast vor Massen-Scoring
    try:
        requests.post(
            API_URL,
            headers=_headers(api_key),
            json={"model": MODEL, "max_tokens": 5,
                  "messages": [{"role": "user", "content": "OK"}]},
            timeout=10,
        ).raise_for_status()
    except Exception as exc:
        logger.error(
            "OpenAI API nicht erreichbar: %s - behalte Keyword-Scores", exc
        )
        return jobs

    # OpenAI cached Prompts >1024 Token automatisch (50 % Rabatt, kein Header nötig)
    system_content = [
        {"role": "system", "content": _system_prompt(context)}
    ]

    # Parallele Verarbeitung in Original-Reihenfolge
    scored: List[Optional[Dict]] = [None] * len(jobs)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        future_to_idx = {
            pool.submit(_score_single, api_key, system_content, job): i
            for i, job in enumerate(jobs)
        }
        for future in as_completed(future_to_idx):
            i = future_to_idx[future]
            result_job, exc = future.result()
            if exc:
                logger.warning(
                    "AI scoring failed for '%s': %s - keeping keyword score",
                    jobs[i].get("title"), exc,
                )
            else:
                logger.debug(
                    "AI score %d/100 for '%s' - %s",
                    result_job["score"], result_job.get("title"), result_job.get("ai_reason"),
                )
            scored[i] = result_job

    ai_scored_count = sum(1 for j in scored if j and "ai_reason" in j)
    logger.info("AI scored %d/%d jobs (parallel, OpenAI auto-caching aktiv)", ai_scored_count, len(jobs))
    return [j for j in scored if j is not None]
