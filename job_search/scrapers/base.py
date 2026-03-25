import logging
import time
from abc import ABC, abstractmethod
from typing import Dict, List

import requests

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}


class BaseScraper(ABC):
    SOURCE_NAME: str = "Unknown"
    POLITE_DELAY: float = 1.5  # seconds between requests

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def get(self, url: str, **kwargs) -> requests.Response:
        """HTTP GET with up to 3 retries and exponential backoff."""
        for attempt in range(3):
            try:
                resp = self.session.get(url, timeout=20, **kwargs)
                resp.raise_for_status()
                return resp
            except requests.RequestException as exc:
                if attempt == 2:
                    raise
                wait = 2 ** attempt
                logger.warning(
                    "%s – attempt %d failed for %s: %s. Retrying in %ds…",
                    self.SOURCE_NAME,
                    attempt + 1,
                    url,
                    exc,
                    wait,
                )
                time.sleep(wait)

    @abstractmethod
    def fetch(self, queries: List[str], location: str) -> List[Dict]:
        """Return list of job dicts.

        Each dict must contain:
            id           – stable unique identifier (hash)
            title        – job title
            company      – company name
            location     – job location string
            url          – direct link to the job posting
            description  – short text snippet (max ~500 chars)
            posted_date  – ISO-ish date string or empty
            source       – SOURCE_NAME
        """
