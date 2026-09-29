"""sec-fundamentals and sec-filings: the parsers on trimmed EDGAR payloads, the
client's throttling and error handling, and the table contracts. The `network`
test at the bottom asks EDGAR for real (needs SEC_USER_AGENT)."""

import os
from datetime import date

import pandas as pd
import pytest

from data_ingest.core.source import Window
from data_ingest.sources import _sec_common as sec
from data_ingest.sources import sec_filings, sec_fundamentals
from data_ingest.sources.sec_filings import SecFilings, parse_filings
from data_ingest.sources.sec_fundamentals import CONCEPTS, SecFundamentals, parse_company_facts

FACTS = {
    "cik": 320193,
    "entityName": "Apple Inc.",
    "facts": {
        "dei": {
            "EntityCommonStockSharesOutstanding": {
                "units": {
                    "shares": [
                        {"end": "2024-10-18", "val": 15115823000, "accn": "0000320193-24-000123",
                         "fy": 2024, "fp": "FY", "form": "10-K", "filed": "2024-11-01"},
                    ]
                }
            }
        },
        "us-gaap": {
            "Assets": {
                "units": {
                    "USD": [
                        {"end": "2024-09-28", "val": 364980000000, "accn": "0000320193-24-000123",
                         "fy": 2024, "fp": "FY", "form": "10-K", "filed": "2024-11-01",
                         "frame": "CY2024Q3I"},
                        # An 8-K is not a report: dropped.
                        {"end": "2024-09-28", "val": 1, "accn": "0000320193-24-000999",
                         "fy": 2024, "fp": "FY", "form": "8-K", "filed": "2024-11-02"},
                    ]
                }
            },
            "NetIncomeLoss": {
                "units": {
                    "USD": [
                        {"start": "2022-09-25", "end": "2023-09-30", "val": 96995000000,
                         "accn": "0000320193-23-000106", "fy": 2023, "fp": "FY", "form": "10-K",
                         "filed": "2023-11-03"},
                        # The same period restated in the next 10-K: kept as its own row.
                        {"start": "2022-09-25", "end": "2023-09-30", "val": 96995000000,
                         "accn": "0000320193-24-000123", "fy": 2024, "fp": "FY", "form": "10-K",
                         "filed": "2024-11-01"},
                    ]
                }
            },
            # Not in CONCEPTS: ignored.
            "SomethingExotic": {"units": {"USD": [
                {"end": "2024-09-28", "val": 5, "accn": "x", "form": "10-K", "filed": "2024-11-01"}
            ]}},
        },
    },
}

SUBMISSIONS = {
    "cik": "320193",
    "name": "Apple Inc.",
    "sic": "3571",
    "sicDescription": "Electronic Computers",
    "filings": {
        "recent": {
            "accessionNumber": ["0000320193-24-000123", "0000320193-24-000120", "0000320193-24-000081"],
            "filingDate": ["2024-11-01", "2024-10-31", "2024-08-02"],
            "reportDate": ["2024-09-28", "", "2024-06-29"],
            "form": ["10-K", "8-K", "10-Q"],
            "primaryDocument": ["aapl-20240928.htm", "a8-k.htm", "aapl-20240629.htm"],
        },
        "files": [],
    },
}


def test_facts_keep_chosen_concepts_and_report_forms_only():
    rows = list(parse_company_facts(FACTS, "AAPL"))
    concepts = sorted({(r[2], r[3]) for r in rows})
    assert concepts == [
        ("dei", "EntityCommonStockSharesOutstanding"),
        ("us-gaap", "Assets"),
        ("us-gaap", "NetIncomeLoss"),
    ]
    assert all(r[11] == "10-K" for r in rows)


def test_instant_and_duration_facts():
    rows = {(r[3], r[13]): r for r in parse_company_facts(FACTS, "AAPL")}
    assets = rows[("Assets", "0000320193-24-000123")]
    assert assets[5] is None and assets[6] == date(2024, 9, 28) and assets[7] == 0
    assert assets[14] == "CY2024Q3I"
    income = rows[("NetIncomeLoss", "0000320193-23-000106")]
    assert income[5] == date(2022, 9, 25) and income[7] == 370


def test_restatements_are_separate_rows_and_filed_since_filters():
    income = [r for r in parse_company_facts(FACTS, "AAPL") if r[3] == "NetIncomeLoss"]
    assert len(income) == 2
    recent = list(parse_company_facts(FACTS, "AAPL", filed_since=date(2024, 1, 1)))
    assert {r[13] for r in recent} == {"0000320193-24-000123"}


def test_fetch_builds_a_frame_matching_the_table(monkeypatch):
    class FakeClient:
        def __init__(self, *a, **k):
            pass

        def get_json(self, url):
            assert url == sec.COMPANY_FACTS_URL.format(cik=320193)
            return FACTS

    monkeypatch.setattr(sec_fundamentals, "SecClient", FakeClient)
    monkeypatch.setattr(sec_fundamentals, "resolve_ciks", lambda client, tickers: {320193: "AAPL"})
    frame = SecFundamentals().fetch(Window.everything())
    assert list(frame.columns) == SecFundamentals.table.column_names
    assert len(frame) == 4
    assert str(frame["fiscal_year"].dtype) == "Int64"
    assert not frame.duplicated(subset=list(SecFundamentals.table.primary_key)).any()


def test_filings_parser_keeps_reports_and_builds_the_document_url():
    rows = list(parse_filings(SUBMISSIONS["filings"]["recent"], SUBMISSIONS, "AAPL"))
    assert [r[3] for r in rows] == ["10-K", "10-Q"]
    ten_k = rows[0]
    assert ten_k[4] == date(2024, 11, 1) and ten_k[5] == date(2024, 9, 28)
    assert ten_k[6] == (
        "https://www.sec.gov/Archives/edgar/data/320193/000032019324000123/aapl-20240928.htm"
    )
    assert ten_k[7:] == ("Apple Inc.", "3571", "Electronic Computers")
    since = list(parse_filings(SUBMISSIONS["filings"]["recent"], SUBMISSIONS, "AAPL", date(2024, 9, 1)))
    assert [r[3] for r in since] == ["10-K"]


def test_filings_fetch(monkeypatch):
    class FakeClient:
        def __init__(self, *a, **k):
            pass

        def get_json(self, url):
            return SUBMISSIONS

    monkeypatch.setattr(sec_filings, "SecClient", FakeClient)
    monkeypatch.setattr(sec_filings, "resolve_ciks", lambda client, tickers: {320193: "AAPL"})
    frame = SecFilings().fetch(Window.everything())
    assert list(frame.columns) == SecFilings.table.column_names
    assert len(frame) == 2


def test_ticker_normalisation_and_resolution():
    assert sec.normalize_ticker("brk.b") == "BRK-B"

    class FakeClient:
        def ticker_to_cik(self):
            return {"BRK-B": 1067983, "GOOGL": 1652044, "GOOG": 1652044}

    # One entry per company, every share class listed; unknown tickers dropped.
    assert sec.resolve_ciks(FakeClient(), ["BRK.B", "GOOGL", "GOOG", "NOPE"]) == {
        1067983: "BRK.B",
        1652044: "GOOG GOOGL",
    }


def test_universe_is_sp500_equities_without_index_symbols():
    tickers = sec.sp500_tickers()
    assert "AAPL" in tickers and len(tickers) > 450
    assert not any(t.startswith("^") for t in tickers)


class _Response:
    def __init__(self, status, body=None):
        self.status_code = status
        self._body = body

    def json(self):
        return self._body


class _Session:
    def __init__(self, responses):
        self.headers = {}
        self.responses = list(responses)

    def get(self, url, timeout):
        return self.responses.pop(0)


def test_client_retries_then_succeeds_and_maps_404_to_none(monkeypatch):
    monkeypatch.setattr(sec.time, "sleep", lambda s: None)
    client = sec.SecClient("Test test@example.com", _Session([_Response(503), _Response(200, {"a": 1}), _Response(404)]))
    assert client.session.headers["User-Agent"] == "Test test@example.com"
    assert client.get_json("u") == {"a": 1}
    assert client.get_json("u") is None


def test_client_explains_a_403(monkeypatch):
    monkeypatch.setattr(sec.time, "sleep", lambda s: None)
    client = sec.SecClient("x", _Session([_Response(403)]))
    with pytest.raises(RuntimeError, match="SEC_USER_AGENT"):
        client.get_json("u")


def test_table_contracts():
    assert SecFundamentals.table.qualified == "fundamentals.sec_facts"
    assert SecFilings.table.qualified == "fundamentals.sec_filings"
    assert "accession" in SecFundamentals.table.primary_key
    assert ("us-gaap", "LongTermDebt") in CONCEPTS
    assert len(set(CONCEPTS)) == len(CONCEPTS)


@pytest.mark.network
@pytest.mark.skipif(not os.getenv("SEC_USER_AGENT"), reason="SEC_USER_AGENT not set")
def test_live_edgar(monkeypatch):
    monkeypatch.setenv("SEC_TICKERS", "AAPL")
    frame = SecFundamentals().fetch(Window(start=date(2023, 1, 1), end=date.today()))
    assert isinstance(frame, pd.DataFrame) and (frame["concept"] == "Assets").any()
