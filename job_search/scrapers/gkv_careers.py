"""
Karriereseiten der deutschen Krankenkassen – direktes Web-Scraping.

Strategie pro Seite:
  1. JSON-LD JobPosting Schema (zuverlässigste Methode, SEO-optimierte Seiten)
  2. Sub-Link zu Stellenangebots-Unterseite folgen
  3. HTML-Fallback (article/div-Cards mit Job-Klassen)
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

GKV_CAREER_PAGES: List[Tuple[str, str]] = [
    ("Techniker Krankenkasse", "https://www.tk.de/karriere"),
    ("BARMER", "https://www.barmer.de/karriere"),
    ("DAK-Gesundheit", "https://www.dak.de/karriere"),
    ("IKK classic", "https://www.ikk-classic.de/karriere"),
    ("KKH", "https://www.kkh.de/karriere"),
    ("SBK", "https://www.sbk.org/karriere/arbeiten-bei-der-sbk"),
    ("hkk", "https://www.hkk.de/karriere"),
    ("BKK firmus", "https://www.bkk-firmus.de/karriere"),
    ("Mobil Krankenkasse", "https://www.mobil-krankenkasse.de/karriere"),
    ("Audi BKK", "https://audibkk.de/portale/karriere"),
    ("VIACTIV", "https://www.viactiv.de/jobs-und-karriere"),
    ("IKK Südwest", "https://www.ikk-suedwest.de/karriere"),
    ("HEK", "https://karriere.hek.de"),
    ("Pronova BKK", "https://www.pronovabkk.de/unternehmen/karriere"),
    ("BAHN-BKK", "https://www.bahn-bkk.de/portale/karriere.html"),
    ("mkk", "https://karriere.meine-krankenkasse.de"),
    ("BIG direkt gesund", "https://www.big-direkt.de/de/karriere"),
    ("mhplus BKK", "https://www.mhplus-krankenkasse.de/job-karriere"),
    ("IKK gesund plus", "https://www.ikk-gesundplus.de/die-ikk/karriere"),
    ("Novitas BKK", "https://karriere.novitas-bkk.de"),
    ("vivida BKK", "https://www.vividabkk.de/karriere"),
    ("BKK Linde", "https://www.bkk-linde.de/karriere"),
    ("IK – Die Innovationskasse", "https://www.die-ik.de/karriere-bei-der-innovationskasse"),
    ("Bosch BKK", "https://www.bosch-bkk.de/karriere"),
    ("IKK Brandenburg und Berlin", "https://www.ikkbb.de/karriere"),
    ("SECURVITA BKK", "https://www.securvita.de/krankenkasse-securvita/stellenangebote.html"),
    ("Debeka BKK", "https://www.debeka-bkk.de/karriere"),
    ("Salus BKK", "https://www.salus-bkk.de/karriere"),
    ("R+V BKK", "https://www.ruv-bkk.de/karriere"),
    ("BKK Gildemeister Seidensticker", "https://www.bkkgs.de/karriere"),
    ("BKK Pfalz", "https://www.bkkpfalz.de/unternehmen/karriere"),
]

# URL/text patterns that indicate a link leads to a job listings sub-page
_JOB_SUBPAGE_PATTERNS = [
    "stellenangebot", "stellenausschreibung", "offene-stelle", "offene_stelle",
    "offene stellen", "aktuelle stellen", "alle stellen",
    "job-board", "jobboerse", "jobbörse", "vakanz", "vakanten",
    "/jobs/", "karriere/jobs", "stellenportal", "job-portal",
]


class GKVCareersScraper(BaseScraper):
    SOURCE_NAME = "GKV Karriere"
    POLITE_DELAY = 2.0

    def fetch(self, queries: List[str], location: str) -> List[Dict]:
        """Scrape all GKV career pages and pre-filter by title keywords."""
        all_jobs: List[Dict] = []
        for company, url in GKV_CAREER_PAGES:
            try:
                jobs = self._scrape(company, url)
                if jobs:
                    logger.debug("%s: %d Stellen gefunden", company, len(jobs))
                all_jobs.extend(jobs)
            except Exception as exc:
                logger.warning("GKV %s: %s", company, exc)
            time.sleep(self.POLITE_DELAY)

        # Pre-filter: keep only jobs whose title contains at least one query keyword
        q_lower = [q.lower() for q in queries]
        filtered = [
            j for j in all_jobs
            if any(kw in j.get("title", "").lower() for kw in q_lower)
        ]
        logger.info(
            "GKV Karriere: %d jobs collected from %d portals → %d after title filter",
            len(all_jobs), len(GKV_CAREER_PAGES), len(filtered),
        )
        return filtered

    # ── per-page scraping ────────────────────────────────────────────────────

    def _scrape(self, company: str, url: str) -> List[Dict]:
        resp = self.get(url)
        soup = BeautifulSoup(resp.text, "lxml")

        # 1 – JSON-LD on the landing page
        jobs = self._from_jsonld(soup, url, company)
        if jobs:
            return jobs

        # 2 – Follow a sub-link to the actual job listing page
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

        # 3 – HTML extraction from the landing page itself
        return self._from_html(soup, url, company)

    # ── extraction strategies ────────────────────────────────────────────────

    def _from_jsonld(
        self, soup: BeautifulSoup, page_url: str, company: str
    ) -> List[Dict]:
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
        """Flatten various JSON-LD structures into a list of JobPosting dicts."""
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

    def _find_jobs_subpage(
        self, soup: BeautifulSoup, base_url: str
    ) -> Optional[str]:
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

    def _from_html(
        self, soup: BeautifulSoup, page_url: str, company: str
    ) -> List[Dict]:
        jobs = []
        seen: set = set()

        # Collect candidate elements: <article>, or elements whose class
        # contains job/stelle/position/career/vacancy/vakanz
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
