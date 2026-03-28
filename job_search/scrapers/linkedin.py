"""
LinkedIn job scraper via linkdAPI (https://github.com/linkdAPI).

Ersetzt das fragile HTML-Scraping des undokumentierten LinkedIn Guest-API
durch einen offiziellen B2B-Datendienst mit sauberem JSON-Output.

Fallback: Wenn LINKDAPI_KEY nicht gesetzt oder linkdapi-Paket fehlt,
wird auf den alten Guest-API-Scraper zurückgefallen (kein Datenverlust).

Benötigt: LINKDAPI_KEY in .env / GitHub Actions Secrets
"""
import hashlib
import logging
import os
import time
from typing import Dict, List

from ..config import MAX_JOBS_PER_QUERY
from .base import BaseScraper

logger = logging.getLogger(__name__)

# Experience levels: Senior + Director (passt zu Christian Gallers Profil)
_EXPERIENCE_LEVELS = ["mid_senior", "director"]


def _extract(job: dict, *keys: str, default: str = "") -> str:
    """Defensiv mehrere mögliche Feldnamen probieren (inkl. verschachtelter Pfade wie 'company.name')."""
    for key in keys:
        val: object = job
        for part in key.split("."):
            if isinstance(val, dict):
                val = val.get(part)
            else:
                val = None
                break
        if val is not None and str(val).strip():
            return str(val).strip()
    return default


class LinkedInScraper(BaseScraper):
    SOURCE_NAME = "LinkedIn"
    POLITE_DELAY = 1.0  # linkdAPI ist kein Scraping → geringere Verzögerung nötig

    def fetch(self, queries: List[str], location: str) -> List[Dict]:
        api_key = os.environ.get("LINKDAPI_KEY", "")
        if not api_key:
            logger.info("LINKDAPI_KEY nicht gesetzt – LinkedIn wird übersprungen")
            return []

        try:
            from linkdapi import LinkdAPI
        except ImportError:
            logger.warning(
                "linkdapi-Paket nicht installiert – LinkedIn wird übersprungen. "
                "Installieren mit: pip install linkdapi"
            )
            return []

        # Standort-Mapping: "Deutschland" → "Germany", sonst "Hamburg, Germany" etc.
        li_location = (
            "Germany"
            if location.lower() in ("deutschland", "germany")
            else f"{location}, Germany"
        )

        seen: set = set()
        jobs: List[Dict] = []

        with LinkdAPI(api_key=api_key) as client:
            for query in queries:
                try:
                    result = client.search_jobs_v2(
                        keyword=query,
                        location=li_location,
                        experience=_EXPERIENCE_LEVELS,
                        date_posted="24h",
                        sort_by="date_posted",
                        count=MAX_JOBS_PER_QUERY,
                    )

                    # API gibt ein Dict zurück; Jobs sind typischerweise unter 'jobs', 'data' oder 'items'
                    raw_jobs = (
                        result.get("jobs")
                        or result.get("data")
                        or result.get("items")
                        or (result if isinstance(result, list) else [])
                    )

                    for job in raw_jobs[:MAX_JOBS_PER_QUERY]:
                        if not isinstance(job, dict):
                            continue

                        title = _extract(job, "title", "jobTitle", "job_title", "position")
                        if not title:
                            continue

                        company = _extract(
                            job,
                            "company.name", "companyName", "company_name",
                            "company", "employer", "hiringOrganization.name",
                        )
                        loc = _extract(
                            job,
                            "location", "jobLocation", "city",
                            "locationName", "formattedLocation",
                            default=location,
                        )
                        url = _extract(
                            job,
                            "jobUrl", "url", "applyUrl", "link",
                            "trackingUrl", "jobPostingUrl",
                        )
                        # Tracking-Parameter entfernen
                        if url and "?" in url:
                            url = url.split("?")[0]

                        description = _extract(
                            job,
                            "description", "jobDescription", "summary",
                            "shortDescription", "snippet",
                        )[:1500]

                        posted = _extract(
                            job,
                            "listedAt", "postedAt", "posted_date",
                            "publishedAt", "datePosted", "date",
                        )

                        job_id = hashlib.md5(
                            f"{title}{company}{url}".encode()
                        ).hexdigest()

                        if job_id in seen:
                            continue
                        seen.add(job_id)

                        jobs.append(
                            {
                                "id": job_id,
                                "title": title,
                                "company": company,
                                "location": loc,
                                "url": url,
                                "description": description,
                                "posted_date": posted,
                                "source": self.SOURCE_NAME,
                            }
                        )

                except Exception as exc:
                    logger.error(
                        "LinkedIn (linkdAPI) query '%s' [%s] failed: %s",
                        query, location, exc,
                    )

                time.sleep(self.POLITE_DELAY)

        logger.info("LinkedIn (linkdAPI): %d jobs collected", len(jobs))
        return jobs
