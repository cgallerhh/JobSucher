"""
StepStone scraper – HTML-Scraping der öffentlichen Suchergebnisseite.
"""
import hashlib
import logging
import time
from typing import Dict, List
from urllib.parse import quote

from bs4 import BeautifulSoup

from ..config import MAX_JOBS_PER_QUERY
from .base import BaseScraper

logger = logging.getLogger(__name__)

BASE_URL = "https://www.stepstone.de"


def _slug(text: str) -> str:
    return (
        text.lower()
        .replace(" ", "-")
        .replace("/", "-")
        .replace("(", "")
        .replace(")", "")
        .replace("ä", "ae")
        .replace("ö", "oe")
        .replace("ü", "ue")
        .replace("ß", "ss")
    )


class StepStoneScraper(BaseScraper):
    SOURCE_NAME = "StepStone"
    POLITE_DELAY = 2.5

    def fetch(self, queries: List[str], location: str) -> List[Dict]:
        seen: set = set()
        jobs: List[Dict] = []

        for query in queries:
            try:
                url = f"{BASE_URL}/jobs/{_slug(query)}/in-{_slug(location)}/"
                resp = self.get(url, params={"radius": "50", "datePosted": "1"})
                soup = BeautifulSoup(resp.text, "lxml")

                articles = soup.find_all("article", attrs={"data-at": "job-item"})
                if not articles:
                    articles = soup.find_all(
                        "article",
                        class_=lambda c: c and "JobCard" in str(c),
                    )

                for article in articles[:MAX_JOBS_PER_QUERY]:
                    try:
                        title_el = article.find(
                            ["h2", "h3"], attrs={"data-at": "job-item-title"}
                        ) or article.find(["h2", "h3"])
                        company_el = article.find(
                            attrs={"data-at": "job-item-company-name"}
                        ) or article.find(
                            class_=lambda c: c and "company" in str(c).lower()
                        )
                        link_el = article.find("a", href=True)
                        loc_el = article.find(attrs={"data-at": "job-item-location"})

                        title = title_el.get_text(strip=True) if title_el else ""
                        if not title:
                            continue

                        href = link_el["href"] if link_el else ""
                        if href and not href.startswith("http"):
                            href = BASE_URL + href

                        job_id = (
                            hashlib.md5(href.encode()).hexdigest()
                            if href
                            else hashlib.md5(title.encode()).hexdigest()
                        )
                        if job_id in seen:
                            continue
                        seen.add(job_id)

                        jobs.append({
                            "id": job_id,
                            "title": title,
                            "company": company_el.get_text(strip=True) if company_el else "",
                            "location": loc_el.get_text(strip=True) if loc_el else location,
                            "url": href,
                            "description": "",
                            "posted_date": "",
                            "source": self.SOURCE_NAME,
                        })
                    except Exception as exc:
                        logger.debug("StepStone card parse error: %s", exc)

            except Exception as exc:
                logger.error("StepStone query '%s' failed: %s", query, exc)

            time.sleep(self.POLITE_DELAY)

        logger.info("StepStone: %d jobs collected", len(jobs))
        return jobs
