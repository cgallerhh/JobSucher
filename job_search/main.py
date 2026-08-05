"""
Job Search Automation – Main entry point.

Run with:  python -m job_search.main
"""
import json
import logging
import os
from collections import Counter
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from pathlib import Path

from dotenv import load_dotenv

# Load .env from repo root when running locally
load_dotenv(Path(__file__).resolve().parent.parent / ".env")
from typing import List, Set

from .ai_scorer import score_jobs_with_ai
from .config import (
    BA_QUERIES,
    EXTERNAL_QUERIES,
    GKV_QUERIES,
    IT_DIENSTLEISTER_QUERIES,
    MANUAL_REVIEW_JOB_IDS,
    MIN_EMAIL_SCORE,
    MAX_JOB_AGE_DAYS,
    PROFILE,
    PROFILE_VERSION,
    SEARCH_LOCATIONS,
)
from .emailer import build_empty_html, build_html, send_email
from .filter import relevance_gate, score_job
from .scrapers.arbeitsagentur import ArbeitsagenturScraper
from .scrapers.gkv_careers import GKVCareersScraper
from .scrapers.indeed import IndeedScraper
from .scrapers.it_dienstleister import ITDienstleisterScraper
from .scrapers.linkedin import LinkedInScraper
from .scrapers.stepstone import StepStoneScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s – %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("job_search")

# A real local cron keeps its operational state outside the Git checkout.  The
# default remains unchanged for GitHub Actions and manual repository runs.
SEEN_FILE = Path(os.environ.get("JOBSUCHER_SEEN_FILE", "data/seen_jobs.json"))
MAX_SEEN_ENTRIES = 5000  # keep file size reasonable
MAX_AI_CANDIDATES = 60
MAX_EMAIL_JOBS = 25


# ── Deduplication helpers ────────────────────────────────────────────────────

def load_seen() -> Set[str]:
    if SEEN_FILE.exists():
        try:
            data = json.loads(SEEN_FILE.read_text())
            if isinstance(data, dict):
                if data.get("profile_version") != PROFILE_VERSION:
                    logger.info(
                        "Search profile changed (%s -> %s); re-evaluating current jobs",
                        data.get("profile_version", "unknown"),
                        PROFILE_VERSION,
                    )
                    return set()
                return set(data.get("job_ids", []))
            if isinstance(data, list):
                logger.info(
                    "Legacy seen-job state found; re-evaluating once for profile %s",
                    PROFILE_VERSION,
                )
                return set()
        except Exception:
            pass
    return set()


def save_seen(seen: Set[str]) -> None:
    SEEN_FILE.parent.mkdir(parents=True, exist_ok=True)
    # Keep only the most recent MAX_SEEN_ENTRIES to prevent unbounded growth
    trimmed = sorted(seen)[-MAX_SEEN_ENTRIES:]
    state = {"profile_version": PROFILE_VERSION, "job_ids": trimmed}
    SEEN_FILE.write_text(json.dumps(state, indent=2) + "\n")


def mark_evaluated_jobs_seen(seen: Set[str], jobs: List[dict]) -> None:
    """Remember every job evaluated with the current search profile."""
    for job in jobs:
        job_id = job.get("id")
        if job_id:
            seen.add(job_id)


def parse_posted_date(value: str):
    """Parse common job-board date formats; return None when a source omits dates."""
    value = (value or "").strip()
    if not value:
        return None

    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(value[:10], fmt).date()
        except ValueError:
            pass

    try:
        parsed = parsedate_to_datetime(value)
        return parsed.date()
    except (TypeError, ValueError):
        return None


def is_fresh_job(job: dict) -> bool:
    """Keep undated jobs, but reject parseable dates older than MAX_JOB_AGE_DAYS."""
    posted = parse_posted_date(job.get("posted_date", ""))
    if posted is None:
        return True
    return posted >= (datetime.now().date() - timedelta(days=MAX_JOB_AGE_DAYS))


def email_gate(job: dict) -> tuple[bool, str]:
    """Entscheide nach der KI, inklusive explizit bestaetigter Prueffaelle."""
    is_manual_review = job.get("id") in MANUAL_REVIEW_JOB_IDS
    gate_score = (
        max(job.get("score", 0), job.get("keyword_score", 0))
        if is_manual_review
        else job.get("score", 0)
    )
    passes_gate, reason = relevance_gate(job, gate_score)
    if not passes_gate:
        return False, reason
    if is_manual_review:
        return True, "manual_review"
    if job.get("score", 0) >= MIN_EMAIL_SCORE:
        return True, "relevant"
    return False, "post_ai_below_70"


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    logger.info("=== Job Search started – %s ===", datetime.now().strftime("%d.%m.%Y %H:%M"))

    seen = load_seen()

    location_agnostic = [GKVCareersScraper(), ITDienstleisterScraper()]

    agnostic_queries = {
        "GKV Karriere":     GKV_QUERIES,
        "Zielunternehmen":  IT_DIENSTLEISTER_QUERIES,
    }

    raw_jobs: List[dict] = []

    # External job boards: all configured locations
    external_scrapers = [ArbeitsagenturScraper, IndeedScraper, StepStoneScraper]
    for location in SEARCH_LOCATIONS:
        for scraper_cls in external_scrapers:
            scraper = scraper_cls()
            try:
                queries = BA_QUERIES if scraper_cls is ArbeitsagenturScraper else EXTERNAL_QUERIES
                jobs = scraper.fetch(queries, location)
                logger.info("%s [%s] → %d jobs fetched", scraper.SOURCE_NAME, location, len(jobs))
                raw_jobs.extend(jobs)
            except Exception as exc:
                logger.error("%s [%s] scraper failed: %s", scraper.SOURCE_NAME, location, exc)

    # LinkedIn: token-free, Germany-wide, restricted to known target companies
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
        "ai_candidates": 0,
        "ai_relevant": 0,
        "final_relevant": 0,
    }

    # Step 1: keyword pre-filter plus hard relevance gate (fast, no API cost)
    candidates: List[dict] = []
    rejected_by_reason: Counter = Counter()
    rejected_by_source: Counter = Counter()
    for job in new_jobs:
        if not is_fresh_job(job):
            rejected_by_reason["too_old"] += 1
            rejected_by_source[job["source"]] += 1
            logger.debug("  FILTERED (too_old): [%s] %s @ %s (%s)",
                         job["source"], job["title"][:60], job["company"][:30],
                         job.get("posted_date", ""))
            continue
        s = score_job(job)
        passes_gate, reason = relevance_gate(job, s)
        if passes_gate:
            candidates.append({**job, "score": s, "keyword_score": s})
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

    if len(candidates) > MAX_AI_CANDIDATES:
        candidates.sort(key=lambda j: j["score"], reverse=True)
        logger.info(
            "Limiting AI scoring to top %d/%d candidates by keyword score",
            MAX_AI_CANDIDATES,
            len(candidates),
        )
        candidates = candidates[:MAX_AI_CANDIDATES]
    diagnostics["ai_candidates"] = len(candidates)

    # Die BA-v6-Ergebnisliste enthaelt nur Berufsbezeichnung und Homeoffice-
    # Hinweis. Hole den Volltext nur fuer die wenigen bereits passenden BA-
    # Kandidaten nach, damit die KI nicht auf einer irrefuehrenden Kurzfassung
    # entscheidet.
    detail_scraper = ArbeitsagenturScraper()
    enriched_candidates: List[dict] = []
    for job in candidates:
        if job.get("source") == "Arbeitsagentur":
            try:
                job = detail_scraper.enrich_details(job)
                keyword_score = score_job(job)
                passes_gate, reason = relevance_gate(job, keyword_score)
                if not passes_gate:
                    rejected_by_reason[f"after_detail_{reason}"] += 1
                    continue
                job = {**job, "score": keyword_score, "keyword_score": keyword_score}
            except Exception as exc:
                logger.warning(
                    "BA details unavailable for '%s': %s - using summary",
                    job.get("title"), exc,
                )
        logger.info(
            "Candidate pre-AI %d/100: %s @ %s",
            job.get("score", 0), job.get("title"), job.get("company"),
        )
        enriched_candidates.append(job)
    candidates = enriched_candidates
    diagnostics["ai_candidates"] = len(candidates)
    diagnostics["rejected_by_reason"] = dict(rejected_by_reason)

    # Step 2: AI re-scoring with full profile context (uses OpenAI API if key present)
    ai_scored = score_jobs_with_ai(candidates)
    diagnostics["ai_relevant"] = len(ai_scored)

    # Re-apply the strict gate after AI scoring (AI may lower or raise some scores)
    relevant: List[dict] = []
    post_ai_rejected: Counter = Counter()
    for job in ai_scored:
        include, reason = email_gate(job)
        if include:
            if reason == "manual_review":
                job = {**job, "manual_review": True}
            relevant.append(job)
        else:
            post_ai_rejected[reason] += 1
    if post_ai_rejected:
        rejected_by_reason.update(post_ai_rejected)
        diagnostics["rejected_by_reason"] = dict(rejected_by_reason)
    relevant.sort(key=lambda j: j["score"], reverse=True)
    if len(relevant) > MAX_EMAIL_JOBS:
        logger.info("Limiting email to top %d/%d relevant jobs", MAX_EMAIL_JOBS, len(relevant))
        relevant = relevant[:MAX_EMAIL_JOBS]
    diagnostics["final_relevant"] = len(relevant)
    logger.info("Relevant after AI scoring: %d", len(relevant))

    # Send first and persist only after Gmail accepted the message. Otherwise a
    # transient mail failure would mark jobs as seen although they were never
    # delivered and they would silently disappear from the next run.
    recipient = os.environ.get("RECIPIENT_EMAIL", PROFILE["email"])
    if relevant:
        subject = (
            f"\U0001f50d {len(relevant)} neue Stelle{'n' if len(relevant) != 1 else ''} "
            f"f\u00fcr dich | {datetime.now().strftime('%d.%m.%Y')}"
        )
        html = build_html(relevant, PROFILE["name"])
        send_email(to=recipient, subject=subject, html=html)
        logger.info("Done – email with %d jobs sent to %s", len(relevant), recipient)
    else:
        subject = f"\U0001f4ed Nullmeldung JobSucher | {datetime.now().strftime('%d.%m.%Y')}"
        html = build_empty_html(PROFILE["name"], diagnostics)
        send_email(to=recipient, subject=subject, html=html)
        logger.info("Done – null report sent to %s", recipient)

    # Remember every job evaluated in this run, including rejected jobs. A future
    # profile-version change deliberately clears this state for one re-evaluation.
    mark_evaluated_jobs_seen(seen, new_jobs)
    save_seen(seen)


if __name__ == "__main__":
    main()
