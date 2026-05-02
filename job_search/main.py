"""
Job Search Automation – Main entry point.

Run with:  python -m job_search.main
"""
import json
import logging
import os
from collections import Counter
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

# Load .env from repo root when running locally
load_dotenv(Path(__file__).resolve().parent.parent / ".env")
from typing import List, Set

from .ai_scorer import score_jobs_with_ai
from .config import EXTERNAL_QUERIES, GKV_QUERIES, IT_DIENSTLEISTER_QUERIES, PROFILE, SEARCH_LOCATIONS
from .emailer import build_empty_html, build_html, send_email
from .filter import relevance_gate, score_job
from .scrapers.arbeitsagentur import ArbeitsagenturScraper
from .scrapers.gkv_careers import GKVCareersScraper
from .scrapers.it_dienstleister import ITDienstleisterScraper
from .scrapers.linkedin import LinkedInScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s – %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("job_search")

SEEN_FILE = Path("data/seen_jobs.json")
MAX_SEEN_ENTRIES = 5000  # keep file size reasonable


# ── Deduplication helpers ────────────────────────────────────────────────────

def load_seen() -> Set[str]:
    if SEEN_FILE.exists():
        try:
            return set(json.loads(SEEN_FILE.read_text()))
        except Exception:
            pass
    return set()


def save_seen(seen: Set[str]) -> None:
    SEEN_FILE.parent.mkdir(parents=True, exist_ok=True)
    # Keep only the most recent MAX_SEEN_ENTRIES to prevent unbounded growth
    trimmed = list(seen)[-MAX_SEEN_ENTRIES:]
    SEEN_FILE.write_text(json.dumps(trimmed, indent=2))


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    logger.info("=== Job Search started – %s ===", datetime.now().strftime("%d.%m.%Y %H:%M"))

    seen = load_seen()

    location_agnostic = [GKVCareersScraper(), ITDienstleisterScraper()]

    agnostic_queries = {
        "GKV Karriere":     GKV_QUERIES,
        "IT Dienstleister": IT_DIENSTLEISTER_QUERIES,
    }

    raw_jobs: List[dict] = []

    # Arbeitsagentur: alle konfigurierten Locations
    for location in SEARCH_LOCATIONS:
        scraper = ArbeitsagenturScraper()
        try:
            jobs = scraper.fetch(EXTERNAL_QUERIES, location)
            logger.info("%s [%s] → %d jobs fetched", scraper.SOURCE_NAME, location, len(jobs))
            raw_jobs.extend(jobs)
        except Exception as exc:
            logger.error("%s [%s] scraper failed: %s", scraper.SOURCE_NAME, location, exc)

    # LinkedIn: nur deutschlandweit (remote) – Hamburg liefert zu wenige Treffer
    linkedin = LinkedInScraper()
    try:
        jobs = linkedin.fetch(EXTERNAL_QUERIES, "Deutschland")
        logger.info("%s [Deutschland/remote] → %d jobs fetched", linkedin.SOURCE_NAME, len(jobs))
        raw_jobs.extend(jobs)
    except Exception as exc:
        logger.error("%s scraper failed: %s", linkedin.SOURCE_NAME, exc)

    # Run location-agnostic scrapers once
    for scraper in location_agnostic:
        try:
            queries = agnostic_queries.get(scraper.SOURCE_NAME, GKV_QUERIES)
            jobs = scraper.fetch(queries, SEARCH_LOCATIONS[0])
            logger.info("%s → %d jobs fetched", scraper.SOURCE_NAME, len(jobs))
            raw_jobs.extend(jobs)
        except Exception as exc:
            logger.error("%s scraper failed: %s", scraper.SOURCE_NAME, exc)

    logger.info("Total raw: %d | Already seen: %d", len(raw_jobs), len(seen))

    # De-duplicate against history
    new_jobs = [j for j in raw_jobs if j["id"] not in seen]
    logger.info("New (not seen before): %d", len(new_jobs))

    # Per-source breakdown after dedup
    src_new = Counter(j["source"] for j in new_jobs)
    src_raw = Counter(j["source"] for j in raw_jobs)
    for src in sorted(src_raw):
        logger.info("  %-20s raw: %2d  new after dedup: %2d  (deduped: %d)",
                    src, src_raw[src], src_new.get(src, 0),
                    src_raw[src] - src_new.get(src, 0))

    diagnostics = {
        "raw_total": len(raw_jobs),
        "seen_total": len(seen),
        "new_total": len(new_jobs),
        "raw_by_source": dict(src_raw),
        "new_by_source": dict(src_new),
        "rejected_by_reason": {},
        "rejected_by_source": {},
        "keyword_candidates": 0,
        "ai_relevant": 0,
        "final_relevant": 0,
    }

    # Step 1: keyword pre-filter plus hard relevance gate (fast, no API cost)
    candidates: List[dict] = []
    rejected_by_reason: Counter = Counter()
    rejected_by_source: Counter = Counter()
    for job in new_jobs:
        s = score_job(job)
        passes_gate, reason = relevance_gate(job, s)
        if passes_gate:
            candidates.append({**job, "score": s})
        else:
            rejected_by_reason[reason] += 1
            rejected_by_source[job["source"]] += 1
            logger.debug("  FILTERED (%s, %2d): [%s] %s @ %s",
                         reason, s, job["source"], job["title"][:60], job["company"][:30])
    diagnostics["keyword_candidates"] = len(candidates)
    diagnostics["rejected_by_reason"] = dict(rejected_by_reason)
    diagnostics["rejected_by_source"] = dict(rejected_by_source)
    logger.info("Candidates after strict relevance gate: %d", len(candidates))
    for reason, cnt in sorted(rejected_by_reason.items()):
        logger.info("  rejected %-26s %d", reason + ":", cnt)

    # Step 2: AI re-scoring with full profile context (uses OpenAI API if key present)
    ai_scored = score_jobs_with_ai(candidates)
    diagnostics["ai_relevant"] = len(ai_scored)

    # Re-apply the strict gate after AI scoring (AI may lower or raise some scores)
    relevant: List[dict] = []
    post_ai_rejected: Counter = Counter()
    for job in ai_scored:
        passes_gate, reason = relevance_gate(job, job["score"])
        if passes_gate:
            relevant.append(job)
        else:
            post_ai_rejected[f"post_ai_{reason}"] += 1
    if post_ai_rejected:
        rejected_by_reason.update(post_ai_rejected)
        diagnostics["rejected_by_reason"] = dict(rejected_by_reason)
    relevant.sort(key=lambda j: j["score"], reverse=True)
    diagnostics["final_relevant"] = len(relevant)
    logger.info("Relevant after AI scoring: %d", len(relevant))

    # Mark only jobs that were actually shown as seen. Rejected jobs may become relevant
    # later if the profile or scoring logic changes.
    for job in relevant:
        seen.add(job["id"])
    save_seen(seen)

    # Send email, including a null-report when nothing relevant was found
    recipient = os.environ.get("RECIPIENT_EMAIL", PROFILE["email"])
    if relevant:
        subject = (
            f"\U0001f50d {len(relevant)} neue Stelle{'n' if len(relevant) != 1 else ''} "
            f"f\u00fcr dich | {datetime.now().strftime('%d.%m.%Y')}"
        )
        html = build_html(relevant, PROFILE["name"])
        try:
            send_email(to=recipient, subject=subject, html=html)
            logger.info("Done – email with %d jobs sent to %s", len(relevant), recipient)
        except Exception as exc:
            logger.error("Failed to send email: %s", exc)
    else:
        subject = f"\U0001f4ed Nullmeldung JobSucher | {datetime.now().strftime('%d.%m.%Y')}"
        html = build_empty_html(PROFILE["name"], diagnostics)
        try:
            send_email(to=recipient, subject=subject, html=html)
            logger.info("Done – null report sent to %s", recipient)
        except Exception as exc:
            logger.error("Failed to send null report email: %s", exc)


if __name__ == "__main__":
    main()
