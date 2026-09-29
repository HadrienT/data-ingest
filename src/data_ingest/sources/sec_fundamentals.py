"""Financial-statement facts from the 10-K and 10-Q filings of S&P 500 companies.

Read from EDGAR's XBRL "company facts" API: every figure a company has tagged
in its filings, with the period it covers, the filing it came from and the
date that filing was made. Two choices shape the table.

**A chosen set of concepts, stored raw.** A company tags hundreds of concepts;
this keeps the ~60 a balance sheet, an income statement, a cash-flow statement
and a structural credit model need (`CONCEPTS`). They are stored under their
XBRL names, untouched: companies disagree on which tag carries "revenue"
(`Revenues`, `RevenueFromContractWithCustomerExcludingAssessedTax`,
`SalesRevenueNet`...), and deciding which one to read is interpretation, which
belongs to the consumer (quant-modeling), not to ingestion.

**One row per (fact, filing).** A 10-K reports the current year and restates
the previous ones, so the same period's revenue appears in several filings,
sometimes with a different value. Keeping each filing's figure, with its
`filed` date, is what lets a backtest ask what was knowable on a given day --
the point-in-time property `fred-macro` gets from VERSIONED, obtained here from
the accession number in the key, since EDGAR itself keeps every vintage.

Instant facts (a balance-sheet figure at a date) have `period_start` NULL and
`duration_days` 0; flow facts (revenue over a quarter or a year) carry both.

A routine run keeps the facts filed in the last `lookback_days`; `--full`
keeps everything since the company started filing XBRL (2009 for the largest).
"""

from __future__ import annotations

import logging
import os
from datetime import date
from typing import Iterable, List, Tuple

import pandas as pd

from ._sec_common import COMPANY_FACTS_URL, REPORT_FORMS, SecClient, resolve_ciks, sp500_tickers
from ..core.source import Source, Window
from ..core.spec import Column, TableSpec, WriteMode

logger = logging.getLogger("data_ingest.sec_fundamentals")

#: (taxonomy, concept). Alternatives for the same line item are all kept on
#: purpose -- see the module docstring.
CONCEPTS: Tuple[Tuple[str, str], ...] = tuple(
    [("dei", "EntityCommonStockSharesOutstanding")]
    + [
        ("us-gaap", c)
        for c in (
            # Income statement
            "Revenues",
            "RevenueFromContractWithCustomerExcludingAssessedTax",
            "RevenueFromContractWithCustomerIncludingAssessedTax",
            "SalesRevenueNet",
            "CostOfRevenue",
            "CostOfGoodsAndServicesSold",
            "GrossProfit",
            "ResearchAndDevelopmentExpense",
            "SellingGeneralAndAdministrativeExpense",
            "OperatingIncomeLoss",
            "InterestExpense",
            "InterestExpenseNonoperating",
            "InterestExpenseDebt",
            "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest",
            "IncomeTaxExpenseBenefit",
            "NetIncomeLoss",
            "EarningsPerShareBasic",
            "EarningsPerShareDiluted",
            "WeightedAverageNumberOfSharesOutstandingBasic",
            "WeightedAverageNumberOfDilutedSharesOutstanding",
            "DepreciationDepletionAndAmortization",
            "DepreciationAndAmortization",
            "DepreciationAmortizationAndAccretionNet",
            # Balance sheet
            "Assets",
            "AssetsCurrent",
            "Liabilities",
            "LiabilitiesCurrent",
            "LiabilitiesAndStockholdersEquity",
            "StockholdersEquity",
            "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
            "RetainedEarningsAccumulatedDeficit",
            "CashAndCashEquivalentsAtCarryingValue",
            "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents",
            "ShortTermInvestments",
            "MarketableSecuritiesCurrent",
            "AccountsReceivableNetCurrent",
            "InventoryNet",
            "Goodwill",
            "PropertyPlantAndEquipmentNet",
            "CommonStockSharesOutstanding",
            # Debt -- what a structural credit model reads
            "DebtCurrent",
            "LongTermDebtCurrent",
            "ShortTermBorrowings",
            "CommercialPaper",
            "LongTermDebt",
            "LongTermDebtNoncurrent",
            "LongTermDebtAndCapitalLeaseObligations",
            "LongTermDebtAndCapitalLeaseObligationsCurrent",
            "OperatingLeaseLiability",
            "OperatingLeaseLiabilityNoncurrent",
            # Cash flow
            "NetCashProvidedByUsedInOperatingActivities",
            "NetCashProvidedByUsedInInvestingActivities",
            "NetCashProvidedByUsedInFinancingActivities",
            "PaymentsToAcquirePropertyPlantAndEquipment",
            "PaymentsOfDividends",
            "PaymentsOfDividendsCommonStock",
            "PaymentsForRepurchaseOfCommonStock",
            "ShareBasedCompensation",
        )
    ]
)


class SecFundamentals(Source):
    name = "sec-fundamentals"
    description = "10-K / 10-Q financial-statement facts for the S&P 500 (SEC EDGAR XBRL)"
    write_mode = WriteMode.UPSERT
    # Reports arrive in waves after each quarter end; a weekly pass over a
    # 30-day window catches every filing, and costs ~500 requests.
    schedule = "0 5 * * 6"
    lookback_days = 30

    table = TableSpec(
        schema="fundamentals",
        name="sec_facts",
        columns=(
            Column("cik", "INTEGER", nullable=False),
            Column("ticker", "TEXT", nullable=False),
            Column("taxonomy", "TEXT", nullable=False),
            Column("concept", "TEXT", nullable=False),
            Column("unit", "TEXT", nullable=False),
            Column("period_start", "DATE"),
            Column("period_end", "DATE", nullable=False),
            Column("duration_days", "INTEGER", nullable=False),
            Column("value", "DOUBLE PRECISION"),
            Column("fiscal_year", "INTEGER"),
            Column("fiscal_period", "TEXT"),
            Column("form", "TEXT", nullable=False),
            Column("filed", "DATE", nullable=False),
            Column("accession", "TEXT", nullable=False),
            Column("frame", "TEXT"),
        ),
        primary_key=(
            "cik", "taxonomy", "concept", "unit", "period_end", "duration_days", "accession",
        ),
        indexes=(("ticker", "concept", "period_end"), ("filed",)),
    )

    def tickers(self) -> List[str]:
        configured = os.getenv("SEC_TICKERS", "")
        if configured.strip():
            return [t.strip() for t in configured.split(",") if t.strip()]
        return sp500_tickers()

    def fetch(self, window: Window) -> pd.DataFrame:
        client = SecClient()
        filed_since = None if window.full else window.start
        frames = []
        for ticker, cik in resolve_ciks(client, self.tickers()).items():
            payload = client.get_json(COMPANY_FACTS_URL.format(cik=cik))
            if payload is None:
                logger.info("%s (CIK %d) has no XBRL facts", ticker, cik)
                continue
            rows = list(parse_company_facts(payload, ticker, filed_since))
            if rows:
                frames.append(pd.DataFrame(rows, columns=self.table.column_names))
        if not frames:
            return pd.DataFrame(columns=self.table.column_names)
        data = pd.concat(frames, ignore_index=True)
        # NULL-able integers: plain pandas would turn them into floats, which
        # COPY refuses for an INTEGER column ("2024.0").
        data["fiscal_year"] = data["fiscal_year"].astype("Int64")
        # A filing sometimes tags the same fact twice (two contexts, one value).
        return data.drop_duplicates(subset=list(self.table.primary_key), keep="first")


def parse_company_facts(
    payload: dict,
    ticker: str,
    filed_since: date | None = None,
    concepts: Iterable[Tuple[str, str]] = CONCEPTS,
    forms: Iterable[str] = REPORT_FORMS,
) -> Iterable[tuple]:
    """Rows (in table column order) for the chosen concepts of one company."""
    cik = int(payload["cik"])
    forms = set(forms)
    facts = payload.get("facts", {})
    for taxonomy, concept in concepts:
        entry = facts.get(taxonomy, {}).get(concept)
        if entry is None:
            continue
        for unit, observations in entry.get("units", {}).items():
            for obs in observations:
                if obs.get("form") not in forms:
                    continue
                filed = date.fromisoformat(obs["filed"])
                if filed_since is not None and filed < filed_since:
                    continue
                end = date.fromisoformat(obs["end"])
                start = date.fromisoformat(obs["start"]) if obs.get("start") else None
                yield (
                    cik,
                    ticker,
                    taxonomy,
                    concept,
                    unit,
                    start,
                    end,
                    (end - start).days if start else 0,
                    float(obs["val"]) if obs.get("val") is not None else None,
                    obs.get("fy"),
                    obs.get("fp"),
                    obs["form"],
                    filed,
                    obs["accn"],
                    obs.get("frame"),
                )
