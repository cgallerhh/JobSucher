"""
Indeed scraper – RSS-Feed via feedparser.

Indeed setzt Session-Cookies beim ersten Besuch. Ohne diese Cookies blockt
Cloudflare den RSS-Request. Fix: Homepage zuerst besuchen, dann RSS fetchen.
"""
import hashlib
import logging
import time
from typing import Dict, List
from urllib.parse import urlencode

import feedparser

from ..config import MAX_JOBS_PER_QUERY
from .base import BaseScraper

logger = logging.getLogger(__name__)

BASE_URL = "https://de.indeed.com"
RSS_URL  = f"{BASE_URL}/jobs"


class IndeedScraper(BaseScraper):
    SOURCE_NAME = "Indeed"
    _warmed_up: bool = False

    def _warmup(self) -> None:
        """Visit homepage once to obtain session cookies."""
        if self._warmed_up:
            return
        try:
            self.session.get(BASE_URL, timeout=10)
            self._warmed_up = True
        except Exception:
            pass

    def fetch(self, queries: List[str], location: str) -> List[Dict]:
        self._warmup()
        seen: set = set()
        jobs: List[Dict] = []

        for query in queries:
            jobs.extend(self._rss(query, location, seen))
            time.sleep(self.POLITE_DELAY)

        logger.info("Indeed: %d jobs collected", len(jobs))
        return jobs

    def _rss(self, query: str, location: str, seen: set) -> List[Dict]:
        try:
            params = {
                "q": query,
                "l": location,
                "sort": "date",
                "fromage": "3",
                "format": "rss",
            }
            url = f"{RSS_URL}?{urlencode(params)}"
            resp = self.session.get(
                url,
                headers={"Accept": "application/rss+xml, application/xml, */*"},
                timeout=15,
            )
            if resp.status_code != 200:
                logger.warning("Indeed: HTTP %s for '%s'", resp.status_code, query)
                return []
            feed = feedparser.parse(resp.text)
            if feed.bozo and not feed.entries:
                logger.warning("Indeed RSS parse error for '%s': %s", query, feed.bozo_exception)
                return []
            jobs = []
            for entry in feed.entries[:MAX_JOBS_PER_QUERY]:
                link = entry.get("link", "")
                job_id = hashlib.md5(link.encode()).hexdigest()
                if job_id in seen:
                    continue
                seen.add(job_id)
                company = entry.get("indeed_company", "")
                if not company and hasattr(entry, "source"):
                    company = getattr(entry.source, "title", "")
                jobs.append({
                    "id": job_id,
                    "title": entry.get("title", "").strip(),
                    "company": company.strip(),
                    "location": entry.get("indeed_formattedlocation", location).strip(),
                    "url": link,
                    "description": entry.get("summary", "")[:500].strip(),
                    "posted_date": entry.get("published", ""),
                    "source": self.SOURCE_NAME,
                })
            return jobs
        except Exception as exc:
            logger.warning("Indeed RSS failed for '%s': %s", query, exc)
            return []
