"""Token-free LinkedIn job search for known target companies.

LinkedIn is intentionally limited to Christian's known GKV insurers and IT
service providers. The scraper uses LinkedIn's public guest job-search HTML and
does not require paid third-party API credits.
"""
from __future__ import annotations

import hashlib
import logging
import re
import time
import unicodedata
from typing import Dict, Iterable, List
from urllib.parse import urlencode

from bs4 import BeautifulSoup

from ..config import MAX_JOBS_PER_QUERY
from .base import BaseScraper
from .gkv_careers import GKV_CAREER_PAGES
from .it_dienstleister import IT_CAREER_PAGES

logger = logging.getLogger(__name__)

BASE_URL = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
GERMANY_GEO_ID = "101282230"

TARGET_COMPANIES = tuple(
    dict.fromkeys(
        [company for company, _ in GKV_CAREER_PAGES]
        + [company for company, _ in IT_CAREER_PAGES]
    )
)

TARGET_ALIASES = {
    "Techniker Krankenkasse": ["techniker krankenkasse", "tk"],
    "BARMER": ["barmer"],
    "DAK-Gesundheit": ["dak", "dak gesundheit"],
    "IKK classic": ["ikk classic"],
    "KKH": ["kkh"],
    "SBK": ["sbk"],
    "hkk": ["hkk"],
    "BKK firmus": ["bkk firmus"],
    "Mobil Krankenkasse": ["mobil krankenkasse"],
    "Audi BKK": ["audi bkk"],
    "VIACTIV": ["viactiv"],
    "IKK Südwest": ["ikk suedwest", "ikk sudwest"],
    "HEK": ["hek"],
    "Pronova BKK": ["pronova bkk"],
    "BAHN-BKK": ["bahn bkk"],
    "mkk": ["mkk", "meine krankenkasse"],
    "BIG direkt gesund": ["big direkt gesund"],
    "mhplus BKK": ["mhplus bkk"],
    "IKK gesund plus": ["ikk gesund plus"],
    "Novitas BKK": ["novitas bkk"],
    "vivida BKK": ["vivida bkk"],
    "BKK Linde": ["bkk linde"],
    "IK – Die Innovationskasse": ["die innovationskasse", "innovationskasse"],
    "Bosch BKK": ["bosch bkk"],
    "IKK Brandenburg und Berlin": ["ikk brandenburg", "ikkbb"],
    "SECURVITA BKK": ["securvita bkk"],
    "Debeka BKK": ["debeka bkk"],
    "Salus BKK": ["salus bkk"],
    "R+V BKK": ["r v bkk", "ruv bkk"],
    "BKK Gildemeister Seidensticker": ["bkk gildemeister seidensticker"],
    "BKK Pfalz": ["bkk pfalz"],
    "Arvato Systems": ["arvato systems"],
    "BITMARCK": ["bitmarck"],
    "ITSC GmbH": ["itsc"],
    "msg systems": ["msg systems", "msg"],
    "CGI": ["cgi"],
    "Dataport": ["dataport"],
    "Sopra Steria": ["sopra steria"],
    "Capgemini": ["capgemini"],
    "Exxeta AG": ["exxeta"],
    "_fbeta GmbH": ["fbeta"],
    "GKV SC GmbH": ["gkv sc"],
    "opta data Gruppe": ["opta data", "optadata"],
}

LEGAL_SUFFIX_RE = re.compile(
    r"\b(ag|se|gmbh|mbh|kg|kgaa|eg|e\.v\.|gruppe|group|holding|deutschland)\b"
)


def _normalize(text: str) -> str:
    text = unicodedata.normalize("NFKD", text or "")
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower().replace("&", " und ").replace("+", " ")
    text = LEGAL_SUFFIX_RE.sub(" ", text)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _contains_company(company: str, aliases: Iterable[str]) -> bool:
    company_norm = f" {_normalize(company)} "
    if not company_norm.strip():
        return False

    for alias in aliases:
        alias_norm = _normalize(alias)
        if not alias_norm:
            continue
        if len(alias_norm) <= 3:
            if re.search(rf"(?<![a-z0-9]){re.escape(alias_norm)}(?![a-z0-9])", company_norm):
                return True
        elif f" {alias_norm} " in company_norm:
            return True
        elif alias_norm in company_norm and len(alias_norm) >= 6:
            return True
    return False


def _target_company(company: str) -> str:
    for target in TARGET_COMPANIES:
        aliases = [target, *TARGET_ALIASES.get(target, [])]
        if _contains_company(company, aliases):
            return target
    return ""


def _text(el, selector: str) -> str:
    found = el.select_one(selector)
    return found.get_text(" ", strip=True) if found else ""


class LinkedInScraper(BaseScraper):
    SOURCE_NAME = "LinkedIn"
    POLITE_DELAY = 2.0
    MAX_QUERY_FAILURES = 4

    def fetch(self, queries: List[str], location: str) -> List[Dict]:
        if location.lower() not in {"deutschland", "germany"}:
            location = "Deutschland"

        seen: set[str] = set()
        jobs: List[Dict] = []
        failures = 0

        for query in queries:
            try:
                fetched = self._fetch_query(query, location, seen)
                jobs.extend(fetched)
                logger.info(
                    "LinkedIn query '%s' [%s] -> %d target-company jobs",
                    query,
                    location,
                    len(fetched),
                )
            except Exception as exc:  # noqa: BLE001 - source is best-effort
                failures += 1
                logger.warning("LinkedIn query '%s' failed: %s", query, exc)
                if failures >= self.MAX_QUERY_FAILURES:
                    logger.warning(
                        "LinkedIn: %d Suchanfragen fehlgeschlagen; Quelle wird fuer diesen Lauf uebersprungen.",
                        failures,
                    )
                    break

            time.sleep(self.POLITE_DELAY)

        logger.info(
            "LinkedIn: %d jobs collected from %d target companies",
            len(jobs),
            len(TARGET_COMPANIES),
        )
        return jobs

    def _fetch_query(self, query: str, location: str, seen: set[str]) -> List[Dict]:
        params = {
            "keywords": query,
            "location": "Germany",
            "geoId": GERMANY_GEO_ID,
            "f_TPR": "r86400",
            "sortBy": "DD",
            "start": "0",
        }
        resp = self.session.get(
            f"{BASE_URL}?{urlencode(params)}",
            headers={"Accept": "text/html, */*"},
            timeout=12,
        )
        if resp.status_code in {403, 429, 999}:
            raise RuntimeError(f"LinkedIn blockt den oeffentlichen Zugriff mit HTTP {resp.status_code}")
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "lxml")
        cards = soup.select("li, .base-card")
        jobs: List[Dict] = []
        for card in cards[:MAX_JOBS_PER_QUERY]:
            title = _text(card, ".base-search-card__title")
            company = _text(card, ".base-search-card__subtitle")
            if not title or not company:
                continue

            matched_target = _target_company(company)
            if not matched_target:
                continue

            link_el = card.select_one("a.base-card__full-link") or card.select_one("a[href]")
            url = link_el.get("href", "").split("?")[0] if link_el else ""
            location_text = _text(card, ".job-search-card__location") or location
            time_el = card.select_one("time")
            posted = time_el.get("datetime", "") if time_el else ""
            entity = card.get("data-entity-urn", "")
            job_id = entity or url or f"{title}{company}{location_text}"
            job_id = hashlib.md5(job_id.encode()).hexdigest()
            if job_id in seen:
                continue
            seen.add(job_id)

            jobs.append(
                {
                    "id": job_id,
                    "title": title,
                    "company": company,
                    "location": location_text,
                    "url": url,
                    "description": f"LinkedIn target company match: {matched_target}",
                    "posted_date": posted,
                    "source": self.SOURCE_NAME,
                    "matched_query": query,
                }
            )
        return jobs
