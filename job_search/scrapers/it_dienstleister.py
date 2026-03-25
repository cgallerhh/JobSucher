"""
Karriereseiten der GKV-IT-Dienstleister – direktes Web-Scraping.

Dieselbe Strategie wie GKVCareersScraper:
  1. JSON-LD JobPosting Schema
  2. Sub-Link zur Stellenangebots-Unterseite folgen
  3. HTML-Fallback (article/div-Cards)
"""
import hashlib
import json
import logging
import time
import warnings
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

from bs4 import XMLParsedAsHTMLWarning
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

from bs4 import BeautifulSoup

from .base import BaseScraper

logger = logging.getLogger(__name__)

IT_CAREER_PAGES: List[Tuple[str, str]] = [
    # Tier 1 – GKV-Kerndienstleister
    # Arvato: JSON-LD auf der Karriereseite vorhanden
    ("Arvato Systems",  "https://www.arvato-systems.de/karriere"),
    # BITMARCK: Direkte Such-/Listenansicht
    ("BITMARCK",        "https://karriere.bitmarck.de/search"),
    # ITSC: kleines Haus, einfaches HTML
    ("ITSC GmbH",       "https://www.itsc.de/karriere"),
    # msg: separater Job-Board auf jobs.msg.group (Hauptseite reagiert mit 406)
    ("msg systems",     "https://jobs.msg.group/en/jobs"),
    # Tier 2 – IT-Beratung mit GKV-Unit
    # CGI: njoyn-Portal mit strukturiertem HTML
    ("CGI",             "https://cgi.njoyn.com/corp/xweb/xweb.asp?page=joblisting&CLID=21001&CountryID=DE&lang=4"),
    # Dataport: öffentlicher IT-Dienstleister, einfaches HTML-Portal
    ("Dataport",        "https://karriere.dataport.de"),
    # Sopra Steria: JSON-LD-Stellenseite
    ("Sopra Steria",    "https://careers.soprasteria.de/jobs"),
    # Capgemini: gefilterte Jobs-Seite Deutschland
    ("Capgemini",       "https://www.capgemini.com/de-de/karriere/jobs/?country_code=de-de&country_name=Germany&size=15"),
    # Tier 4 – Spezialisten & Mittelständler
    ("_fbeta GmbH",     "https://fbeta.de/karriere/"),
    # GKV SC GmbH: korrekte Domain www1.gkvsc.de
    ("GKV SC GmbH",     "https://www1.gkvsc.de/karriere/stellenangebote/"),
    # opta data Gruppe: GKV-IT-Dienstleister
    ("opta data Gruppe", "https://karriere.optadata.de/search"),
]

_JOB_SUBPAGE_PATTERNS = [
    "stellenangebot", "stellenausschreibung", "offene-stelle", "offene_stelle",
    "offene stellen", "aktuelle stellen", "alle stellen",
    "job-board", "jobboerse", "jobbörse", "vakanz", "vakanten",
    "/jobs/", "karriere/jobs", "stellenportal", "job-portal",
]


class ITDienstleisterScraper(BaseScraper):
    SOURCE_NAME = "IT Dienstleister"
    POLITE_DELAY = 2.0

    def fetch(self, queries: List[str], location: str) -> List[Dict]:
        """Scrape alle IT-Dienstleister-Karriereseiten, filtern nach Titel-Keywords."""
        all_jobs: List[Dict] = []
        for company, url in IT_CAREER_PAGES:
            try:
                jobs = self._scrape(company, url)
                if jobs:
                    logger.debug("%s: %d Stellen gefunden", company, len(jobs))
                all_jobs.extend(jobs)
            except Exception as exc:
                logger.warning("IT Dienstleister %s: %s", company, exc)
            time.sleep(self.POLITE_DELAY)

        # Titelfilter: mindestens ein Query-Keyword muss im Titel vorkommen
        q_lower = [q.lower() for q in queries]
        filtered = [
            j for j in all_jobs
            if any(kw in j.get("title", "").lower() for kw in q_lower)
        ]
        logger.info(
            "IT Dienstleister: %d jobs collected from %d portals → %d after title filter",
            len(all_jobs), len(IT_CAREER_PAGES), len(filtered),
        )
        return filtered

    # ── per-page scraping ────────────────────────────────────────────────────

    def _scrape(self, company: str, url: str) -> List[Dict]:
        resp = self.get(url)
        soup = BeautifulSoup(resp.text, "lxml")

        # 1 – JSON-LD auf der Landing-Page
        jobs = self._from_jsonld(soup, url, company)
        if jobs:
            return jobs

        # 2 – Sub-Link zur eigentlichen Stellenliste folgen
        sub_url = self._find_jobs_subpage(soup, url)
        if sub_url:
            resp2 = self.get(sub_url)
            ct = resp2.headers.get("content-type", "")
            parser = "xml" if "xml" in ct else "lxml"
            soup2 = BeautifulSoup(resp2.text, parser)
            jobs = self._from_jsonld(soup2, sub_url, company)
            if jobs:
                return jobs
            jobs = self._from_html(soup2, sub_url, company)
            if jobs:
                return jobs

        # 3 – HTML-Fallback auf der Landing-Page
        return self._from_html(soup, url, company)

    # ── Extraktionsstrategien ────────────────────────────────────────────────

    def _from_jsonld(self, soup: BeautifulSoup, page_url: str, company: str) -> List[Dict]:
        jobs = []
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string or "")
            except Exception:
                continue
            postings = self._collect_postings(data)
            for p in postings:
                title = (p.get("title") or "").strip()
                if not title:
                    continue
                link = p.get("url") or page_url
                emp = p.get("hiringOrganization") or {}
                comp = (emp.get("name") if isinstance(emp, dict) else "") or company
                loc = self._location_from_jsonld(p)
                jobs.append({
                    "id": hashlib.md5(link.encode()).hexdigest(),
                    "title": title,
                    "company": comp,
                    "location": loc or "Deutschland",
                    "url": link,
                    "description": (p.get("description") or "")[:500].strip(),
                    "posted_date": p.get("datePosted", ""),
                    "source": self.SOURCE_NAME,
                })
        return jobs

    def _collect_postings(self, data) -> List[dict]:
        if isinstance(data, dict):
            t = data.get("@type", "")
            if t == "JobPosting":
                return [data]
            if t == "ItemList":
                out = []
                for el in data.get("itemListElement") or []:
                    item = el.get("item", el) if isinstance(el, dict) else el
                    if isinstance(item, dict) and item.get("@type") == "JobPosting":
                        out.append(item)
                return out
        if isinstance(data, list):
            return [d for d in data if isinstance(d, dict) and d.get("@type") == "JobPosting"]
        return []

    def _location_from_jsonld(self, posting: dict) -> str:
        loc = posting.get("jobLocation")
        if not loc:
            return ""
        if isinstance(loc, list):
            loc = loc[0]
        if isinstance(loc, dict):
            addr = loc.get("address") or {}
            if isinstance(addr, dict):
                return addr.get("addressLocality") or addr.get("addressRegion") or ""
            return str(addr)
        return ""

    def _find_jobs_subpage(self, soup: BeautifulSoup, base_url: str) -> Optional[str]:
        base_domain = urlparse(base_url).netloc
        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True).lower()
            href_l = href.lower()
            if any(p in href_l or p in text for p in _JOB_SUBPAGE_PATTERNS):
                full = urljoin(base_url, href)
                if urlparse(full).netloc == base_domain and full != base_url:
                    return full
        return None

    def _from_html(self, soup: BeautifulSoup, page_url: str, company: str) -> List[Dict]:
        jobs = []
        seen: set = set()

        def _job_class(c):
            if not c:
                return False
            joined = " ".join(c).lower()
            return any(
                p in joined
                for p in ["job", "stelle", "position", "career", "vacancy", "vakanz"]
            )

        candidates = soup.find_all("article") + soup.find_all(class_=_job_class)

        for el in candidates[:60]:
            try:
                title_el = el.find(["h1", "h2", "h3", "h4"]) or el.find(
                    class_=lambda c: c and any(
                        p in " ".join(c).lower()
                        for p in ["title", "titel", "heading", "name"]
                    )
                )
                if not title_el:
                    continue
                title = title_el.get_text(strip=True)
                if not (8 <= len(title) <= 180):
                    continue

                link_el = el.find("a", href=True)
                href = ""
                if link_el:
                    href = urljoin(page_url, link_el["href"])
                elif el.name == "a" and el.get("href"):
                    href = urljoin(page_url, el["href"])

                job_id = hashlib.md5((href or title).encode()).hexdigest()
                if job_id in seen:
                    continue
                seen.add(job_id)

                loc_el = el.find(
                    class_=lambda c: c and any(
                        p in " ".join(c).lower()
                        for p in ["location", "ort", "standort", "city"]
                    )
                )
                jobs.append({
                    "id": job_id,
                    "title": title,
                    "company": company,
                    "location": loc_el.get_text(strip=True) if loc_el else "Deutschland",
                    "url": href,
                    "description": el.get_text(separator=" ", strip=True)[:400],
                    "posted_date": "",
                    "source": self.SOURCE_NAME,
                })
            except Exception:
                pass
        return jobs
