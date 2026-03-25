"""
Bundesagentur für Arbeit – offizielle REST-API v4.
Authentifizierung: X-API-Key Header (kein OAuth mehr).

Doku: https://jobsuche.api.bund.dev
"""
import logging
import time
from typing import Dict, List

from ..config import MAX_JOBS_PER_QUERY
from .base import BaseScraper

logger = logging.getLogger(__name__)

BASE_URL = "https://rest.arbeitsagentur.de/jobboerse/jobsuche-service/pc/v4/jobs"
API_KEY  = "jobboerse-jobsuche"


class ArbeitsagenturScraper(BaseScraper):
    SOURCE_NAME = "Arbeitsagentur"
    POLITE_DELAY = 1.0

    def __init__(self) -> None:
        super().__init__()
        # Use a clean session with only API-compatible headers (no browser-specific headers)
        import requests as _req
        self._api_session = _req.Session()
        self._api_session.headers.update({
            "X-API-Key": API_KEY,
            "Accept": "application/json",
        })

    def fetch(self, queries: List[str], location: str) -> List[Dict]:
        seen: set = set()
        jobs: List[Dict] = []

        for query in queries:
            try:
                params: dict = {
                    "was": query,
                    "veroeffentlichtseit": "3",
                    "size": str(MAX_JOBS_PER_QUERY),
                    "page": "1",
                }
                if location.lower() != "deutschland":
                    params["wo"] = location
                    params["umkreis"] = "50"
                resp = self._api_session.get(BASE_URL, params=params,
                    timeout=20,
                )
                if not resp.ok:
                    logger.error(
                        "Arbeitsagentur %d für '%s' – %s",
                        resp.status_code, query, resp.text[:300],
                    )
                    continue

                for offer in resp.json().get("stellenangebote") or []:
                    ref = offer.get("refnr", "")
                    job_id = ref or f"{offer.get('titel','')}{offer.get('arbeitgeber','')}"
                    if job_id in seen:
                        continue
                    seen.add(job_id)

                    url = offer.get("externeUrl") or (
                        f"https://www.arbeitsagentur.de/jobsuche/jobdetail/{ref}"
                        if ref else ""
                    )
                    arbeitsort = offer.get("arbeitsort", {})
                    job_location = ", ".join(
                        filter(None, [arbeitsort.get("ort"), arbeitsort.get("region")])
                    ) or location

                    jobs.append({
                        "id": job_id,
                        "title": offer.get("titel", "").strip(),
                        "company": offer.get("arbeitgeber", "").strip(),
                        "location": job_location,
                        "url": url,
                        "description": (offer.get("stellenbeschreibung") or "")[:500].strip(),
                        "posted_date": offer.get("aktuelleVeroeffentlichungsdatum", ""),
                        "source": self.SOURCE_NAME,
                    })

            except Exception as exc:
                logger.error("Arbeitsagentur query '%s' failed: %s", query, exc)

            time.sleep(self.POLITE_DELAY)

        logger.info("Arbeitsagentur: %d jobs collected", len(jobs))
        return jobs
