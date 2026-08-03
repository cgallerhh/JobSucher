"""
Bundesagentur für Arbeit – Jobsuche REST-API v6.
Authentifizierung: X-API-Key Header (kein OAuth mehr).

Doku: https://jobsuche.api.bund.dev
"""
import logging
import time
from typing import Dict, List

from ..config import MAX_JOB_AGE_DAYS, MAX_JOBS_PER_QUERY
from .base import BaseScraper

logger = logging.getLogger(__name__)

BASE_URL = "https://rest.arbeitsagentur.de/jobboerse/jobsuche-service/pc/v6/jobs"
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
                    "veroeffentlichtseit": str(MAX_JOB_AGE_DAYS),
                    "size": str(MAX_JOBS_PER_QUERY),
                    "page": "1",
                    "angebotsart": "1",
                    "pav": "false",
                    "zeitarbeit": "false",
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

                payload = resp.json()
                offers = payload.get("ergebnisliste") or payload.get("stellenangebote") or []
                for offer in offers:
                    ref = offer.get("referenznummer") or offer.get("refnr", "")
                    title = offer.get("stellenangebotsTitel") or offer.get("titel", "")
                    company = offer.get("firma") or offer.get("arbeitgeber", "")
                    job_id = ref or f"{title}{company}"
                    if job_id in seen:
                        continue
                    seen.add(job_id)

                    url = offer.get("externeURL") or offer.get("externeUrl") or (
                        f"https://www.arbeitsagentur.de/jobsuche/jobdetail/{ref}"
                        if ref else ""
                    )
                    locations = offer.get("stellenlokationen") or []
                    address = (locations[0].get("adresse") if locations else None) or {}
                    legacy_location = offer.get("arbeitsort") or {}
                    job_location = ", ".join(filter(None, [
                        address.get("ort") or legacy_location.get("ort"),
                        address.get("region") or legacy_location.get("region"),
                    ])) or location

                    salary_from = offer.get("gehaltsspanneVon")
                    salary_to = offer.get("gehaltsspanneBis")
                    summary_parts = [offer.get("hauptberuf") or ""]
                    if salary_from or salary_to:
                        summary_parts.append(
                            f"Jahresgehalt {salary_from or '?'} bis {salary_to or '?'} EUR"
                        )
                    if offer.get("homeofficemoeglich"):
                        summary_parts.append("Homeoffice moeglich; Umfang unklar")
                    description = " | ".join(part for part in summary_parts if part)

                    jobs.append({
                        "id": job_id,
                        "title": title.strip(),
                        "company": company.strip(),
                        "location": job_location,
                        "url": url,
                        "description": description[:500],
                        "posted_date": (
                            offer.get("datumErsteVeroeffentlichung")
                            or (offer.get("veroeffentlichungszeitraum") or {}).get("von")
                            or offer.get("aktuelleVeroeffentlichungsdatum", "")
                        ),
                        "salary_min": salary_from,
                        "salary_max": salary_to,
                        "source": self.SOURCE_NAME,
                        "matched_query": query,
                    })

            except Exception as exc:
                logger.error("Arbeitsagentur query '%s' failed: %s", query, exc)

            time.sleep(self.POLITE_DELAY)

        logger.info("Arbeitsagentur: %d jobs collected", len(jobs))
        return jobs
