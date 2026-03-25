"""
LinkedIn scraper – uses the public guest-jobs API endpoint.
No login required; filter: past 24 hours (f_TPR=r86400).
"""
import hashlib
import logging
import time
from typing import Dict, List
from urllib.parse import urlencode

from bs4 import BeautifulSoup

from ..config import MAX_JOBS_PER_QUERY
from .base import BaseScraper

logger = logging.getLogger(__name__)

GUEST_API = (
    "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
)


class LinkedInScraper(BaseScraper):
    SOURCE_NAME = "LinkedIn"
    POLITE_DELAY = 3.0  # LinkedIn is more aggressive about rate-limiting

    def fetch(self, queries: List[str], location: str) -> List[Dict]:
        seen: set = set()
        jobs: List[Dict] = []

        # LinkedIn needs a slightly different Accept header
        self.session.headers.update({"Accept": "text/html, */*; q=0.01"})

        for query in queries:
            try:
                li_location = "Germany" if location.lower() == "deutschland" else f"{location}, Germany"
                params = {
                    "keywords": query,
                    "location": li_location,
                    "f_TPR": "r86400",  # posted in last 24h
                    "start": "0",
                    "count": str(MAX_JOBS_PER_QUERY),
                }
                url = f"{GUEST_API}?{urlencode(params)}"
                resp = self.get(url)
                soup = BeautifulSoup(resp.text, "lxml")

                # Each result is in a <li> containing a div.job-search-card
                cards = soup.find_all(
                    "div", class_=lambda c: c and "job-search-card" in str(c)
                )
                if not cards:
                    cards = soup.find_all("li")

                for card in cards[:MAX_JOBS_PER_QUERY]:
                    try:
                        title_el = card.find("h3") or card.find("h2")
                        company_el = card.find("h4") or card.find(
                            class_=lambda c: c
                            and "subtitle" in str(c).lower()
                        )
                        link_el = card.find("a", href=True)
                        loc_el = card.find(
                            "span",
                            class_=lambda c: c and "location" in str(c).lower(),
                        )

                        title = title_el.get_text(strip=True) if title_el else ""
                        if not title:
                            continue

                        href = link_el["href"] if link_el else ""
                        # Strip LinkedIn tracking params
                        if href and "?" in href:
                            href = href.split("?")[0]

                        company = (
                            company_el.get_text(strip=True) if company_el else ""
                        )
                        job_id = hashlib.md5(
                            f"{title}{company}{href}".encode()
                        ).hexdigest()

                        if job_id in seen:
                            continue
                        seen.add(job_id)

                        jobs.append(
                            {
                                "id": job_id,
                                "title": title,
                                "company": company,
                                "location": loc_el.get_text(strip=True)
                                if loc_el
                                else location,
                                "url": href,
                                "description": "",
                                "posted_date": "",
                                "source": self.SOURCE_NAME,
                            }
                        )
                    except Exception as exc:
                        logger.debug("LinkedIn card parse error: %s", exc)

            except Exception as exc:
                logger.error("LinkedIn query '%s' failed: %s", query, exc)

            time.sleep(self.POLITE_DELAY)

        logger.info("LinkedIn: %d jobs collected", len(jobs))
        return jobs
