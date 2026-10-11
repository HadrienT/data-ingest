"""Regenerate the S&P 500 membership history read by `sp500-membership` —
src/data_ingest/sources/data/sp500_membership.csv.

Run by hand when the index changes, review the diff, commit it:

    python scripts/refresh_sp500_membership.py

A committed file for the same reasons as the constituent lists (see
refresh_index_constituents.py): the history changes only through a reviewed
commit, and the ingestion needs no network.

Sources — free, and reusable:
  1996-01-02 to its last update   github.com/fja05680/sp500 (MIT), file
                                  sp500_ticker_start_end.csv: one row per
                                  stay of a ticker in the index, built from
                                  the daily lists of A. Clenow's "Trading
                                  Evolved" and kept up from Wikipedia
  after that                      English Wikipedia (CC BY-SA), "Historical
                                  components of the S&P 500": the dated
                                  additions and removals
  check                           English Wikipedia, "List of S&P 500
                                  companies": the result must end on exactly
                                  today's members, or nothing is written

A change of symbol is a removal and an addition on the same day, the way the
base file writes it. Wikipedia does not date them: one that happened since
the base file's last update goes in RENAMES, and is dated by this run.
"""

from __future__ import annotations

import io
import sys
from datetime import date
from pathlib import Path

import pandas as pd
import requests

OUT = Path(__file__).resolve().parents[1] / "src" / "data_ingest" / "sources" / "data" / "sp500_membership.csv"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; data-ingest; +https://github.com/HadrienT/data-ingest)"}

BASE = "https://raw.githubusercontent.com/fja05680/sp500/master/sp500_ticker_start_end.csv"
CHANGES = "https://en.wikipedia.org/wiki/Historical_components_of_the_S%26P_500"
CURRENT = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

#: old symbol -> new symbol, for changes of symbol more recent than the base
#: file. Remove an entry once the base file has it.
RENAMES = {
    "PSKY": "SKYD",  # Paramount Skydance, renamed Skydance Corporation
}

#: The index has had between 487 and 507 lines since 1996.
MEMBERS = range(480, 520)


def _get(url: str) -> str:
    response = requests.get(url, headers=HEADERS, timeout=60)
    response.raise_for_status()
    return response.text


def _yahoo(symbol: str) -> str:
    """A share class is written with a dash on Yahoo (BRK.B -> BRK-B)."""
    return str(symbol).strip().replace(".", "-")


def base() -> pd.DataFrame:
    frame = pd.read_csv(io.StringIO(_get(BASE)), dtype=str, keep_default_na=False)
    frame = frame.rename(columns={"start_date": "date_added", "end_date": "date_removed"})
    frame["ticker"] = frame["ticker"].map(_yahoo)
    return frame[["ticker", "date_added", "date_removed"]]


def changes() -> pd.DataFrame:
    """Wikipedia's additions and removals: date, added, removed ('' for none)."""
    table = next(t for t in pd.read_html(io.StringIO(_get(CHANGES))) if "Effective Date" in str(t.columns[0]))
    frame = pd.DataFrame(
        {
            "date": pd.to_datetime(table.iloc[:, 0], errors="coerce").dt.date,
            "added": table.iloc[:, 1].fillna("").map(_yahoo),
            "removed": table.iloc[:, 3].fillna("").map(_yahoo),
        }
    )
    if frame["date"].isna().any():
        raise RuntimeError("a change on Wikipedia has no readable date")
    return frame.sort_values("date").reset_index(drop=True)


def current() -> set[str]:
    table = next(t for t in pd.read_html(io.StringIO(_get(CURRENT))) if "Symbol" in t.columns)
    return {_yahoo(s) for s in table["Symbol"]}


def build(history: pd.DataFrame, later: pd.DataFrame, renames: dict, today: str) -> pd.DataFrame:
    """The base history, then the changes that came after it, then the renames."""
    rows = history.to_dict("records")

    def close(ticker: str, day: str) -> None:
        stays = [r for r in rows if r["ticker"] == ticker and r["date_removed"] == ""]
        if len(stays) != 1:
            raise RuntimeError(f"{ticker} removed on {day} but it has {len(stays)} open stays")
        stays[0]["date_removed"] = day

    def open_(ticker: str, day: str) -> None:
        if any(r["ticker"] == ticker and r["date_removed"] == "" for r in rows):
            raise RuntimeError(f"{ticker} added on {day} but it is already a member")
        rows.append({"ticker": ticker, "date_added": day, "date_removed": ""})

    for change in later.itertuples(index=False):
        day = change.date.isoformat()
        if change.removed:
            close(change.removed, day)
        if change.added:
            open_(change.added, day)
    for old, new in renames.items():
        if any(r["ticker"] == old and r["date_removed"] == "" for r in rows):
            close(old, today)
            open_(new, today)
    return pd.DataFrame(rows).sort_values(["ticker", "date_added"]).reset_index(drop=True)


def members_on(history: pd.DataFrame, day: str) -> set[str]:
    """Who was in the index on `day`: added on or before it, not yet removed."""
    inside = (history["date_added"] <= day) & ((history["date_removed"] == "") | (history["date_removed"] > day))
    return set(history.loc[inside, "ticker"])


def main() -> int:
    today = date.today().isoformat()
    history = base()
    last = max(history["date_added"].max(), history["date_removed"].max())
    later = changes()
    later = later[later["date"].map(date.isoformat) > last]
    print(f"base file: {len(history)} stays up to {last}; {len(later)} later changes on Wikipedia")

    result = build(history, later, RENAMES, today)
    members, expected = members_on(result, today), current()
    if members != expected:
        print(f"the history does not end on today's index:\n  in it, not in the index: {sorted(members - expected)}"
              f"\n  in the index, not in it: {sorted(expected - members)}\n"
              "A change of symbol goes in RENAMES. Nothing written.", file=sys.stderr)
        return 1
    if len(members) not in MEMBERS:
        print(f"{len(members)} members: not the S&P 500. Nothing written.", file=sys.stderr)
        return 1

    result["as_of"] = today
    result.to_csv(OUT, index=False)
    print(f"{OUT.name}: {len(result)} stays, {result['ticker'].nunique()} tickers, {len(members)} members today")
    return 0


if __name__ == "__main__":
    sys.exit(main())
