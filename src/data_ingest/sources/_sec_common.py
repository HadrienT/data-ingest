"""Shared plumbing for the SEC EDGAR sources (sec-fundamentals, sec-filings).

EDGAR is free and needs no key, but it has two rules, both enforced:

* every request carries a User-Agent naming who is asking, with a contact
  address ("Name email@example.com"); a request without one gets a 403.
  It comes from SEC_USER_AGENT so no address lives in this public repo;
* no more than ten requests per second, per host. `SecClient` spaces its
  requests well under that, and backs off on 429 and 5xx.

Leading underscore so the registry's auto-discovery skips this module.
"""

from __future__ import annotations

import logging
import time
from typing import Dict, List, Optional

import pandas as pd
import requests

from ..config import DATA_DIR, require

logger = logging.getLogger("data_ingest.sec_common")

TICKER_MAP_URL = "https://www.sec.gov/files/company_tickers.json"
COMPANY_FACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik:010d}.json"
SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik:010d}.json"
SUBMISSIONS_PAGE_URL = "https://data.sec.gov/submissions/{name}"
ARCHIVE_URL = "https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/{document}"

#: Seconds between two requests: ~7 per second, under the SEC's limit of 10.
MIN_INTERVAL = 0.15
MAX_RETRIES = 4
RETRY_BACKOFF_SECONDS = 2.0

#: The annual and quarterly reports, and their amendments. 20-F / 40-F
#: (foreign filers) are left out: the S&P 500 files 10-Ks.
REPORT_FORMS = ("10-K", "10-K/A", "10-Q", "10-Q/A")


def normalize_ticker(ticker: str) -> str:
    """One spelling for share classes: BRK.B, BRK-B and brk-b all -> BRK-B."""
    return ticker.strip().upper().replace(".", "-")


def sp500_tickers() -> List[str]:
    """The S&P 500 equities of data/tickers.csv (the ^ index symbols dropped)."""
    tickers = pd.read_csv(DATA_DIR / "tickers.csv", header=None)[0].dropna().astype(str)
    return [t for t in tickers if not t.startswith("^")]


class SecClient:
    """A throttled, retrying GET against EDGAR, with the mandatory User-Agent."""

    def __init__(self, user_agent: Optional[str] = None, session: Optional[requests.Session] = None):
        self.session = session or requests.Session()
        self.session.headers.update(
            {
                "User-Agent": user_agent or require("SEC_USER_AGENT"),
                "Accept-Encoding": "gzip, deflate",
            }
        )
        self._last = 0.0

    def get_json(self, url: str) -> Optional[dict]:
        """The decoded body, or None for a 404 (a CIK with no XBRL facts)."""
        for attempt in range(1, MAX_RETRIES + 1):
            wait = MIN_INTERVAL - (time.monotonic() - self._last)
            if wait > 0:
                time.sleep(wait)
            self._last = time.monotonic()
            try:
                response = self.session.get(url, timeout=60)
            except requests.RequestException as exc:
                logger.warning("%s: %s (attempt %d)", url, exc, attempt)
            else:
                if response.status_code == 404:
                    return None
                if response.status_code == 403:
                    raise RuntimeError(
                        "EDGAR answered 403: SEC_USER_AGENT must name you and give a "
                        "contact address, e.g. 'Jane Doe jane@example.com'."
                    )
                if response.status_code < 400:
                    return response.json()
                logger.warning("%s: HTTP %d (attempt %d)", url, response.status_code, attempt)
            time.sleep(RETRY_BACKOFF_SECONDS * attempt)
        raise RuntimeError(f"EDGAR request failed {MAX_RETRIES} times: {url}")

    def ticker_to_cik(self) -> Dict[str, int]:
        """Every ticker EDGAR knows, normalised, mapped to its CIK."""
        payload = self.get_json(TICKER_MAP_URL) or {}
        return {normalize_ticker(row["ticker"]): int(row["cik_str"]) for row in payload.values()}


def resolve_ciks(client: SecClient, tickers: List[str]) -> Dict[str, int]:
    """CIK per ticker; a ticker EDGAR does not list is logged and skipped."""
    known = client.ticker_to_cik()
    resolved: Dict[str, int] = {}
    for ticker in tickers:
        cik = known.get(normalize_ticker(ticker))
        if cik is None:
            logger.warning("No CIK for %s; skipped", ticker)
        else:
            resolved[ticker] = cik
    return resolved
