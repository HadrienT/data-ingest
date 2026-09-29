"""The 10-K and 10-Q filings of S&P 500 companies: which, when, and a link.

From EDGAR's "submissions" API. It complements `sec-fundamentals`: the facts
table says what a filing reported, this one says what the filing is -- its
form, filing date, the period it covers, and the URL of the document itself,
so a figure on quant-modeling's credit page can link to the page it came from.
The company's name and industry (SIC) ride along on each row, as EDGAR
reports them at fetch time.

A routine run reads the ~1000 most recent filings EDGAR returns inline, which
reach back years even for companies that file often; `--full` also walks the
older pages EDGAR splits the rest of the history into.
"""

from __future__ import annotations

import logging
import os
from datetime import date
from typing import Iterable, List

import pandas as pd

from ._sec_common import (
    ARCHIVE_URL,
    REPORT_FORMS,
    SUBMISSIONS_PAGE_URL,
    SUBMISSIONS_URL,
    SecClient,
    resolve_ciks,
    sp500_tickers,
)
from ..core.source import Source, Window
from ..core.spec import Column, TableSpec, WriteMode

logger = logging.getLogger("data_ingest.sec_filings")


class SecFilings(Source):
    name = "sec-filings"
    description = "10-K / 10-Q filing index for the S&P 500, with document links (SEC EDGAR)"
    write_mode = WriteMode.UPSERT
    schedule = "30 4 * * 6"
    lookback_days = 30

    table = TableSpec(
        schema="fundamentals",
        name="sec_filings",
        columns=(
            Column("cik", "INTEGER", nullable=False),
            Column("ticker", "TEXT", nullable=False),
            Column("accession", "TEXT", nullable=False),
            Column("form", "TEXT", nullable=False),
            Column("filed", "DATE", nullable=False),
            Column("report_date", "DATE"),
            Column("url", "TEXT"),
            Column("entity_name", "TEXT"),
            Column("sic", "TEXT"),
            Column("sic_description", "TEXT"),
        ),
        primary_key=("cik", "accession"),
        indexes=(("ticker", "filed"),),
    )

    def tickers(self) -> List[str]:
        configured = os.getenv("SEC_TICKERS", "")
        if configured.strip():
            return [t.strip() for t in configured.split(",") if t.strip()]
        return sp500_tickers()

    def fetch(self, window: Window) -> pd.DataFrame:
        client = SecClient()
        filed_since = None if window.full else window.start
        rows: list = []
        for ticker, cik in resolve_ciks(client, self.tickers()).items():
            payload = client.get_json(SUBMISSIONS_URL.format(cik=cik))
            if payload is None:
                continue
            pages = [payload["filings"]["recent"]]
            if window.full:
                for extra in payload["filings"].get("files", []):
                    page = client.get_json(SUBMISSIONS_PAGE_URL.format(name=extra["name"]))
                    if page is not None:
                        pages.append(page)
            for page in pages:
                rows.extend(parse_filings(page, payload, ticker, filed_since))
        frame = pd.DataFrame(rows, columns=self.table.column_names)
        return frame.drop_duplicates(subset=list(self.table.primary_key), keep="first")


def parse_filings(
    page: dict,
    company: dict,
    ticker: str,
    filed_since: date | None = None,
    forms: Iterable[str] = REPORT_FORMS,
) -> Iterable[tuple]:
    """Rows for the report filings of one submissions page (parallel arrays)."""
    cik = int(company["cik"])
    forms = set(forms)
    for i, form in enumerate(page.get("form", [])):
        if form not in forms:
            continue
        filed = date.fromisoformat(page["filingDate"][i])
        if filed_since is not None and filed < filed_since:
            continue
        accession = page["accessionNumber"][i]
        report = page.get("reportDate", [""] * (i + 1))[i]
        document = page.get("primaryDocument", [""] * (i + 1))[i]
        yield (
            cik,
            ticker,
            accession,
            form,
            filed,
            date.fromisoformat(report) if report else None,
            ARCHIVE_URL.format(cik=cik, accession=accession.replace("-", ""), document=document)
            if document
            else None,
            company.get("name"),
            company.get("sic") or None,
            company.get("sicDescription") or None,
        )
