"""
Karriereseiten der priorisierten GKV-, Healthcare- und Enterprise-Tech-Ziele.

Dieselbe Strategie wie GKVCareersScraper:
  1. JSON-LD JobPosting Schema
  2. Sub-Link zur Stellenangebots-Unterseite folgen
  3. HTML-Fallback (article/div-Cards)
"""
import hashlib
import json
import logging
import math
import re
import time
import warnings
from datetime import date, timedelta
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

from bs4 import XMLParsedAsHTMLWarning
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

from bs4 import BeautifulSoup

from .base import BaseScraper

logger = logging.getLogger(__name__)

IT_CAREER_PAGES: List[Tuple[str, str]] = [
    # A1/A2 des Masterdokuments
    ("DeepL", "https://jobs.ashbyhq.com/deepl"),
    ("SAP", "https://jobs.sap.com/go/Germany/8806101/"),
    ("ZOTZ|KLIMAS", "https://jobapplication.hrworks.de/de?companyId=x228f1b"),
    ("FREENOW", "https://job-boards.greenhouse.io/freenow"),
    ("Genesys", "https://genesys.wd1.myworkdayjobs.com/Genesys"),
    ("Veeam", "https://job-boards.eu.greenhouse.io/veeamsoftware"),
    ("Salesforce", "https://careers.salesforce.com/de/jobs/"),
    ("Thieme", "https://jobs.thieme.com/"),
    # Kontrollierte Enterprise-Tech-Transferpipeline
    ("SoftwareOne", "https://careers.softwareone.com/"),
    ("Amazon Web Services (AWS)", "https://www.amazon.jobs/en/teams/amazon-web-services"),
    ("Camunda", "https://jobs.ashbyhq.com/camunda"),
    ("Public Cloud Group", "https://jobs.ashbyhq.com/publiccloudgroup"),
    ("AppZen", "https://jobs.lever.co/appzen"),
    ("Gartner", "https://jobs.gartner.com/search-jobs"),
    ("SnapLogic", "https://jobs.lever.co/snaplogic"),
    ("BLP Digital", "https://jobs.ashbyhq.com/blp-digital"),
    ("Ashby", "https://jobs.ashbyhq.com/ashby"),
    ("360Learning", "https://jobs.lever.co/360learning"),
    ("Conceptboard", "https://conceptboard.jobs.personio.com/"),
    # Breite Marktbeobachtung ohne Firmen aus dem Ausschlussregister
    ("ITSC GmbH", "https://www.itsc.de/karriere"),
    ("msg systems", "https://jobs.msg.group/en/jobs"),
    ("CGI", "https://cgi.njoyn.com/corp/xweb/xweb.asp?page=joblisting&CLID=21001&CountryID=DE&lang=4"),
    ("Dataport", "https://karriere.dataport.de"),
    ("Sopra Steria", "https://careers.soprasteria.de/jobs"),
    ("Capgemini", "https://www.capgemini.com/de-de/karriere/jobs/?country_code=de-de&country_name=Germany&size=15"),
    ("_fbeta GmbH", "https://fbeta.de/karriere/"),
    ("GKV SC GmbH", "https://www1.gkvsc.de/karriere/stellenangebote/"),
    ("opta data Gruppe", "https://karriere.optadata.de/search"),
]

_JOB_SUBPAGE_PATTERNS = [
    "stellenangebot", "stellenausschreibung", "offene-stelle", "offene_stelle",
    "offene stellen", "aktuelle stellen", "alle stellen",
    "job-board", "jobboerse", "jobbörse", "vakanz", "vakanten",
    "/jobs/", "karriere/jobs", "stellenportal", "job-portal",
]

MY_JOB_SHOP_CONFIG = {}


class ITDienstleisterScraper(BaseScraper):
    SOURCE_NAME = "Zielunternehmen"
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
                logger.warning("Zielunternehmen %s: %s", company, exc)
            time.sleep(self.POLITE_DELAY)

        # Titelfilter: mindestens ein Query-Keyword muss im Titel vorkommen
        q_lower = [q.lower() for q in queries]
        filtered = [
            j for j in all_jobs
            if any(kw in j.get("title", "").lower() for kw in q_lower)
        ]
        logger.info(
            "Zielunternehmen: %d jobs collected from %d portals → %d after title filter",
            len(all_jobs), len(IT_CAREER_PAGES), len(filtered),
        )
        return filtered

    # ── per-page scraping ────────────────────────────────────────────────────

    def _scrape(self, company: str, url: str) -> List[Dict]:
        # Moderne Hosted Boards stellen veroeffentlichte Jobs ueber oeffentliche
        # JSON-Endpunkte bereit; deren HTML-Landingpages sind meist clientseitig.
        jobs = self._from_hosted_board_api(company, url)
        if jobs:
            return jobs

        # Talentsconnect/Job-Shop pages render job cards client-side. Use the
        # public active-offers search endpoint exposed by the page instead.
        jobs = self._from_my_job_shop_api(company, url)
        if jobs:
            return jobs

        resp = self.get(url)
        soup = BeautifulSoup(resp.text, "lxml")

        if urlparse(url).netloc == "jobapplication.hrworks.de":
            jobs = self._from_hrworks(soup, url, company)
            if jobs:
                return jobs

        if urlparse(url).netloc == "jobs.sap.com":
            jobs = self._from_sap_pages(soup, url, company)
            if jobs:
                return jobs

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

    def _from_hosted_board_api(self, company: str, page_url: str) -> List[Dict]:
        parsed = urlparse(page_url)
        board_name = parsed.path.strip("/").split("/")[0]
        if not board_name:
            return []

        if parsed.netloc == "jobs.ashbyhq.com":
            return self._from_ashby(company, board_name)
        if "greenhouse.io" in parsed.netloc:
            return self._from_greenhouse(company, board_name)
        if parsed.netloc in {"jobs.lever.co", "jobs.eu.lever.co"}:
            return self._from_lever(company, board_name, parsed.netloc)
        if parsed.netloc.endswith(".myworkdayjobs.com"):
            return self._from_workday(company, page_url)
        return []

    def _from_workday(self, company: str, page_url: str) -> List[Dict]:
        """Lese ein oeffentliches Workday-Karriereportal seitenweise aus."""
        parsed = urlparse(page_url)
        tenant = parsed.netloc.split(".")[0]
        board = parsed.path.strip("/").split("/")[0]
        if not tenant or not board:
            return []

        api_url = f"https://{parsed.netloc}/wday/cxs/{tenant}/{board}/jobs"
        limit = 20
        offset = 0
        postings: List[Dict] = []
        while offset < 200:
            resp = self.session.post(
                api_url,
                json={
                    "appliedFacets": {},
                    "limit": limit,
                    "offset": offset,
                    "searchText": "",
                },
                timeout=20,
            )
            resp.raise_for_status()
            payload = resp.json() or {}
            page = payload.get("jobPostings") or []
            postings.extend(page)
            offset += len(page)
            if not page or offset >= int(payload.get("total") or len(postings)):
                break

        jobs: List[Dict] = []
        for posting in postings:
            title = (posting.get("title") or "").strip()
            external_path = posting.get("externalPath") or ""
            if not title or not external_path:
                continue
            link = urljoin(f"https://{parsed.netloc}", external_path)
            bullet_fields = posting.get("bulletFields") or []
            jobs.append({
                "id": hashlib.md5(link.encode()).hexdigest(),
                "title": title,
                "company": company,
                "location": posting.get("locationsText") or "Unbekannt",
                "url": link,
                "description": " | ".join(str(value) for value in bullet_fields if value)[:1500],
                "posted_date": self._workday_posted_date(posting.get("postedOn") or ""),
                "source": self.SOURCE_NAME,
            })
        return jobs

    def _workday_posted_date(self, posted_on: str) -> str:
        """Uebersetze Workday-Angaben wie 'Posted 3 Days Ago' in ISO-Daten."""
        match = re.search(r"(\d+)\s+day", posted_on, flags=re.IGNORECASE)
        if match:
            return (date.today() - timedelta(days=int(match.group(1)))).isoformat()
        if re.search(r"today", posted_on, flags=re.IGNORECASE):
            return date.today().isoformat()
        return posted_on

    def _from_ashby(self, company: str, board_name: str) -> List[Dict]:
        resp = self.session.get(
            f"https://api.ashbyhq.com/posting-api/job-board/{board_name}",
            params={"includeCompensation": "true"},
            timeout=20,
        )
        resp.raise_for_status()
        jobs: List[Dict] = []
        for posting in resp.json().get("jobs") or []:
            if not posting.get("isListed", True):
                continue
            location = posting.get("location") or ""
            if posting.get("isRemote") and "remote" not in location.lower():
                location = f"{location} / Remote".strip(" /")
            compensation = posting.get("compensation") or {}
            salary_summary = compensation.get("scrapeableCompensationSalarySummary") or ""
            description = posting.get("descriptionPlain") or ""
            if salary_summary:
                description = f"{salary_summary} | {description}"
            jobs.append({
                "id": posting.get("id") or hashlib.md5(posting.get("jobUrl", "").encode()).hexdigest(),
                "title": (posting.get("title") or "").strip(),
                "company": company,
                "location": location or "Unbekannt",
                "url": posting.get("jobUrl") or "",
                "description": description[:1500].strip(),
                "posted_date": posting.get("publishedAt") or "",
                "source": self.SOURCE_NAME,
            })
        return jobs

    def _from_greenhouse(self, company: str, board_name: str) -> List[Dict]:
        resp = self.session.get(
            f"https://boards-api.greenhouse.io/v1/boards/{board_name}/jobs",
            params={"content": "true"},
            timeout=20,
        )
        resp.raise_for_status()
        jobs: List[Dict] = []
        for posting in resp.json().get("jobs") or []:
            content = BeautifulSoup(posting.get("content") or "", "lxml").get_text(" ", strip=True)
            jobs.append({
                "id": str(posting.get("id") or hashlib.md5(posting.get("absolute_url", "").encode()).hexdigest()),
                "title": (posting.get("title") or "").strip(),
                "company": company,
                "location": (posting.get("location") or {}).get("name") or "Unbekannt",
                "url": posting.get("absolute_url") or "",
                "description": content[:1500],
                "posted_date": posting.get("updated_at") or "",
                "source": self.SOURCE_NAME,
            })
        return jobs

    def _from_lever(self, company: str, board_name: str, host: str) -> List[Dict]:
        api_host = "api.eu.lever.co" if host == "jobs.eu.lever.co" else "api.lever.co"
        resp = self.session.get(
            f"https://{api_host}/v0/postings/{board_name}",
            params={"mode": "json"},
            timeout=20,
        )
        resp.raise_for_status()
        jobs: List[Dict] = []
        for posting in resp.json() or []:
            categories = posting.get("categories") or {}
            location = categories.get("location") or ""
            if posting.get("workplaceType") == "remote" and "remote" not in location.lower():
                location = f"{location} / Remote".strip(" /")
            description = posting.get("descriptionPlain") or ""
            salary = posting.get("salaryRange") or {}
            annual_salary = (
                salary.get("currency") == "EUR"
                and str(salary.get("interval") or "").lower()
                in {"year", "annual", "annually", "yearly", "per-year"}
            )
            salary_min = salary.get("min") if annual_salary else None
            salary_max = salary.get("max") if annual_salary else None
            if salary_min or salary_max:
                description = f"Verguetung {salary_min or '?'} bis {salary_max or '?'} EUR | {description}"
            jobs.append({
                "id": posting.get("id") or hashlib.md5(posting.get("hostedUrl", "").encode()).hexdigest(),
                "title": (posting.get("text") or "").strip(),
                "company": company,
                "location": location or "Unbekannt",
                "url": posting.get("hostedUrl") or posting.get("applyUrl") or "",
                "description": description[:1500].strip(),
                "posted_date": "",
                "salary_min": salary_min,
                "salary_max": salary_max,
                "source": self.SOURCE_NAME,
            })
        return jobs

    def _from_sap(self, soup: BeautifulSoup, page_url: str, company: str) -> List[Dict]:
        """Extrahiere die serverseitig ausgegebenen SAP-SuccessFactors-Zeilen."""
        jobs: List[Dict] = []
        seen: set[str] = set()
        rows = soup.select("tr.data-row, li.job-tile, div.job")
        if not rows:
            rows = [link.parent for link in soup.select('a.jobTitle-link, a[href*="/job/"]')]

        for row in rows:
            if row is None:
                continue
            link_el = row.select_one('a.jobTitle-link, a[href*="/job/"]')
            if not link_el or not link_el.get("href"):
                continue
            title = link_el.get_text(" ", strip=True)
            if not title:
                continue
            link = urljoin(page_url, link_el["href"])
            job_id = hashlib.md5(link.encode()).hexdigest()
            if job_id in seen:
                continue
            seen.add(job_id)
            location_el = row.select_one(
                ".jobLocation, .job-location, .location, [class*='location']"
            )
            jobs.append({
                "id": job_id,
                "title": title,
                "company": company,
                "location": location_el.get_text(" ", strip=True) if location_el else "Deutschland",
                "url": link,
                "description": row.get_text(" ", strip=True)[:1500],
                "posted_date": "",
                "source": self.SOURCE_NAME,
            })
        return jobs

    def _from_sap_pages(
        self,
        first_soup: BeautifulSoup,
        page_url: str,
        company: str,
    ) -> List[Dict]:
        """Lese alle Ergebnis-Seiten der deutschen SAP-Stellenliste."""
        jobs = self._from_sap(first_soup, page_url, company)
        last_link = first_soup.select_one("a.paginationItemLast[href]")
        if not last_link:
            return jobs

        last_path = urlparse(urljoin(page_url, last_link["href"])).path.rstrip("/")
        try:
            last_offset = int(last_path.rsplit("/", 1)[-1])
        except ValueError:
            return jobs

        parsed = urlparse(page_url)
        base_path = parsed.path.rstrip("/")
        for offset in range(25, min(last_offset, 475) + 1, 25):
            next_url = (
                f"{parsed.scheme}://{parsed.netloc}{base_path}/{offset}/"
                "?q=&sortColumn=referencedate&sortDirection=desc"
            )
            resp = self.get(next_url)
            next_soup = BeautifulSoup(resp.text, "lxml")
            jobs.extend(self._from_sap(next_soup, next_url, company))
            time.sleep(0.3)

        unique: Dict[str, Dict] = {}
        for job in jobs:
            unique[job["id"]] = job
        return list(unique.values())

    def _from_hrworks(
        self,
        soup: BeautifulSoup,
        page_url: str,
        company: str,
    ) -> List[Dict]:
        """Extrahiere Karten aus dem oeffentlichen HRworks-Bewerbungsportal."""
        jobs: List[Dict] = []
        seen: set[str] = set()
        for title_link in soup.select("a.job-offer-content[title][href]"):
            title = (title_link.get("title") or "").strip()
            link = urljoin(page_url, title_link.get("href") or "")
            if not title or not link or link in seen:
                continue
            seen.add(link)

            card = title_link.find_parent("div", class_="portlet") or title_link.parent
            location_parts: List[str] = []
            for icon_class in ["icomoon-location", "icomoon-home"]:
                icon = card.select_one(f"i.{icon_class}") if card else None
                container = icon.find_parent("a") if icon else None
                value = container.get_text(" ", strip=True) if container else ""
                if value and value not in location_parts:
                    location_parts.append(value)

            description = card.get_text(" ", strip=True) if card else title
            jobs.append({
                "id": hashlib.md5(link.encode()).hexdigest(),
                "title": title,
                "company": company,
                "location": " / ".join(location_parts) or "Unbekannt",
                "url": link,
                "description": description[:1500],
                "posted_date": "",
                "source": self.SOURCE_NAME,
            })
        return jobs

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
            parts = [self._location_from_jsonld({**posting, "jobLocation": item}) for item in loc]
            return ", ".join(dict.fromkeys(part for part in parts if part))
        if isinstance(loc, dict):
            addr = loc.get("address") or {}
            if isinstance(addr, dict):
                return addr.get("addressLocality") or addr.get("addressRegion") or ""
            return str(addr)
        return ""

    def _from_my_job_shop_api(self, company: str, page_url: str) -> List[Dict]:
        cfg = MY_JOB_SHOP_CONFIG.get(urlparse(page_url).netloc)
        if not cfg:
            return []

        tenant_id = cfg["tenant_id"]
        vanity = cfg["vanity"]
        job_shop_id = cfg["job_shop_id"]
        api_base = "https://api.my-job-shop.com"

        key_resp = self.session.get(
            f"{api_base}/api/offer/v1/search/api-key",
            headers={"Accept": "application/json", "X-Tenant-Id": tenant_id},
            params={"filter": f"backoffice_vanity:{vanity}"},
            timeout=20,
        )
        key_resp.raise_for_status()
        api_key = (key_resp.json() or {}).get("key")
        if not api_key:
            logger.warning("%s: Job-Shop API key missing", company)
            return []

        def fetch_page(page: int) -> dict:
            body = {
                "searches": [
                    {
                        "collection": "offers",
                        "q": "*",
                        "filter_by": (
                            f"tenant_id:={tenant_id}"
                            f"&&backoffice_vanity:={vanity}"
                            "&&status:=ACTIVE"
                        ),
                        "page": page,
                        "per_page": 250,
                    }
                ]
            }
            resp = self.session.post(
                f"{api_base}/api/typesense/multi_search",
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                    "X-Tenant-Id": tenant_id,
                    "X-JobShop-Id": job_shop_id,
                    "X-Typesense-Api-Key": api_key,
                },
                data=json.dumps(body),
                timeout=20,
            )
            resp.raise_for_status()
            return (resp.json().get("results") or [{}])[0]

        result = fetch_page(1)
        if result.get("error"):
            logger.warning("%s: Job-Shop search failed: %s", company, result["error"])
            return []

        hits = result.get("hits") or []
        found = result.get("found") or len(hits)
        pages = min(math.ceil(found / 250), 5)
        for page in range(2, pages + 1):
            hits.extend(fetch_page(page).get("hits") or [])

        jobs: List[Dict] = []
        for hit in hits:
            doc = hit.get("document") or {}
            title = (doc.get("title") or "").strip()
            if not title:
                continue

            link = doc.get("url") or doc.get("application_url") or page_url
            location = self._stringify_list(doc.get("location")) or "Deutschland"
            description = self._job_shop_description(doc)
            job_id = doc.get("offer_uuid") or doc.get("id") or hashlib.md5(link.encode()).hexdigest()

            jobs.append({
                "id": job_id,
                "title": title,
                "company": company,
                "location": location,
                "url": link,
                "description": description[:500],
                "posted_date": "",
                "source": self.SOURCE_NAME,
            })

        return jobs

    def _job_shop_description(self, doc: dict) -> str:
        html_parts = [
            self._stringify_list(doc.get("department")),
            doc.get("introduction") or "",
            doc.get("description") or "",
            doc.get("expectation") or "",
            doc.get("offering") or "",
        ]
        raw = " ".join(part for part in html_parts if part)
        text = BeautifulSoup(raw, "lxml").get_text(" ", strip=True)
        return re.sub(r"\s+", " ", text)

    def _stringify_list(self, value) -> str:
        if isinstance(value, list):
            return ", ".join(str(item) for item in value if item)
        return str(value or "")

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

        def _class_text(value) -> str:
            if isinstance(value, str):
                return value.lower()
            return " ".join(value or []).lower()

        def _job_class(c):
            if not c:
                return False
            joined = _class_text(c)
            return any(
                p in joined
                for p in ["job", "stelle", "position", "career", "vacancy", "vakanz"]
            )

        candidates = soup.find_all("article") + soup.find_all(
            lambda tag: tag.has_attr("class") and _job_class(tag.get("class"))
        )

        for el in candidates[:60]:
            try:
                title_el = el.find(["h1", "h2", "h3", "h4"]) or el.find(
                    class_=lambda c: c and any(
                        p in _class_text(c)
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
                        p in _class_text(c)
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
