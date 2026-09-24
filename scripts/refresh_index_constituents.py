"""Regenerate the index constituent lists read by `equity-universe` and
`intl-equity-prices` — src/data_ingest/sources/data/constituents/*.csv.

Run by hand when an index rebalances (a few times a year), review the diff,
commit it:

    python scripts/refresh_index_constituents.py

Why a committed file rather than scraping at every run: the list then changes
only through a reviewed commit, a Wikipedia vandalism or layout change cannot
silently empty a market overnight, and the ingestion itself needs no network
beyond the price download. The same reason `sp500-prices` reads tickers.csv.

Sources — free, and reusable (Wikipedia, CC BY-SA):
  CAC 40, DAX, FTSE 100  English Wikipedia constituent tables
  Nikkei 225             Japanese Wikipedia (the English page has no table;
                         Nikkei's own list is marked "copying prohibited")

Each row is checked against Yahoo Finance: the symbol must return prices, and
its quote currency is recorded — London lines quote in pence (GBp), which the
price source converts to pounds. Names come from Wikipedia, except Nikkei's
(Japanese on the page), taken from Yahoo's English short name.
"""

from __future__ import annotations

import csv
import io
import re
import sys
from datetime import date
from pathlib import Path

import pandas as pd
import requests
import yfinance as yf

OUT = Path(__file__).resolve().parents[1] / "src" / "data_ingest" / "sources" / "data" / "constituents"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; data-ingest; +https://github.com/HadrienT/data-ingest)"}

#: market id -> (Yahoo symbol of the index, its name)
INDICES = {
    "CAC40": ("^FCHI", "CAC 40"),
    "DAX": ("^GDAXI", "DAX"),
    "FTSE100": ("^FTSE", "FTSE 100"),
    "NIKKEI225": ("^N225", "Nikkei 225"),
}
#: A list shorter than this is a parsing failure, never written.
EXPECTED = {"CAC40": 40, "DAX": 40, "FTSE100": 100, "NIKKEI225": 225}


def _tables(url: str) -> list[pd.DataFrame]:
    html = requests.get(url, headers=HEADERS, timeout=60).text
    return pd.read_html(io.StringIO(html))


def _table_with(tables: list[pd.DataFrame], column: str, rows: int) -> pd.DataFrame:
    for t in tables:
        cols = [str(c) for c in t.columns]
        if any(column in c for c in cols) and len(t) == rows:
            return t
    raise RuntimeError(f"no table with a {column!r} column and {rows} rows")


def cac40() -> list[tuple[str, str]]:
    t = _table_with(_tables("https://en.wikipedia.org/wiki/CAC_40"), "Ticker", 40)
    return [(str(r["Ticker"]).strip(), str(r["Company"]).strip()) for _, r in t.iterrows()]


def dax() -> list[tuple[str, str]]:
    t = _table_with(_tables("https://en.wikipedia.org/wiki/DAX"), "Ticker", 40)
    return [(str(r["Ticker"]).strip(), str(r["Company"]).strip()) for _, r in t.iterrows()]


def ftse100() -> list[tuple[str, str]]:
    t = _table_with(_tables("https://en.wikipedia.org/wiki/FTSE_100_Index"), "Ticker", 100)
    # Yahoo writes a share class with a dash (BT.A -> BT-A.L).
    return [(str(r["Ticker"]).strip().replace(".", "-") + ".L", str(r["Company"]).strip()) for _, r in t.iterrows()]


def nikkei225() -> list[tuple[str, str]]:
    tables = _tables("https://ja.wikipedia.org/wiki/%E6%97%A5%E7%B5%8C%E5%B9%B3%E5%9D%87%E6%A0%AA%E4%BE%A1")
    codes: list[str] = []
    for t in tables:
        cols = [c for c in t.columns if "コード" in str(c)]
        # The constituents are split in sector tables of a few dozen rows; the
        # page's other code tables (additions/removals by year) are wider.
        if cols and len(t.columns) == 3:
            codes += [str(x).strip() for x in t[cols[0]] if re.fullmatch(r"\d{3}[0-9A-Z]", str(x).strip())]
    codes = list(dict.fromkeys(codes))
    return [(f"{c}.T", "") for c in codes]


def _yahoo(symbols: list[str]) -> dict[str, tuple[str, str]]:
    """symbol -> (quote currency, English short name), for symbols that trade."""
    out: dict[str, tuple[str, str]] = {}
    for s in symbols:
        tk = yf.Ticker(s)
        try:
            ccy = tk.fast_info.get("currency")
            if tk.history(period="5d").empty or not ccy:
                continue
            name = ""
            try:
                name = tk.info.get("shortName") or tk.info.get("longName") or ""
            except Exception:  # noqa: BLE001 — a name is a nicety, the price is not
                pass
            out[s] = (ccy, name)
        except Exception as exc:  # noqa: BLE001
            print(f"  {s}: {exc}", file=sys.stderr)
    return out


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    today = date.today().isoformat()
    ok = True
    for market, scrape in (("CAC40", cac40), ("DAX", dax), ("FTSE100", ftse100), ("NIKKEI225", nikkei225)):
        members = scrape()
        index_symbol, index_name = INDICES[market]
        yahoo = _yahoo([index_symbol] + [s for s, _ in members])
        missing = [s for s, _ in members if s not in yahoo]
        print(f"{market}: {len(members)} members, {len(missing)} without Yahoo prices {missing}")
        if len(members) != EXPECTED[market] or missing:
            print(f"  NOT written: expected {EXPECTED[market]} tradeable members", file=sys.stderr)
            ok = False
            continue
        path = OUT / f"{market.lower()}.csv"
        with path.open("w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(["ticker", "name", "kind", "quote_currency", "as_of"])
            w.writerow([index_symbol, index_name, "index", yahoo[index_symbol][0], today])
            for symbol, name in sorted(members):
                w.writerow([symbol, name or yahoo[symbol][1], "equity", yahoo[symbol][0], today])
        print(f"  wrote {path.relative_to(OUT.parents[3])}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
