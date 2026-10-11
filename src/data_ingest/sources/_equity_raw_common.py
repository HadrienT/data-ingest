"""Prices as quoted and corporate actions, shared by `equity-raw-prices`,
`equity-dividends` and `equity-splits`.

Leading underscore so the registry's auto-discovery skips this module.

A backtest keeps a book in shares and cash: it credits a dividend in cash on
its ex-date and multiplies the position on a split. It therefore needs the
price **quoted that day**, adjusted for nothing, with the events apart. Yahoo
does not serve that. Its unadjusted bars (`auto_adjust=False`) are still
restated for every later split, and so are its dividends: Apple closed at
111.94 on 2000-01-03 and Yahoo says 0.9994, after four splits worth 112 to 1.

The three tables undo that restatement. With `F(t)` the product of the ratios
of the splits taking effect strictly after day `t`:

- price quoted on `t`      = Yahoo's price × `F(t)`
- volume traded on `t`     = Yahoo's volume ÷ `F(t)`
- dividend detached on `e` = Yahoo's dividend × `F(e)`, per share **as
  counted on the ex-date**
- a split's ratio is new shares per old share: 2 for a two-for-one, 0.1 for a
  one-for-ten reverse split.

A stored row therefore never changes when a later split happens, which is
what makes a backtest run again in six months read the same numbers.

The total return of day `t`, with `P` the close, `D` the dividend and `s` the
ratio taking effect that day (0 and 1 when there is none):

    s (P_t + D_t) / P_{t-1} - 1

Yahoo's own adjusted close gives `s P_t / (P_{t-1} - s D_t) - 1`: the same
events, the dividend reinvested at the previous close rather than at the
ex-date's. The two differ by about (D/P)² on an ex-date, 1e-4 for a usual
quarterly dividend. `scripts/verify_raw_prices.py` checks the stored rows
against a fresh download with both formulas.

What Yahoo calls a split is not always one: **a spin-off comes as a split
with an odd ratio** (GE, 1.281 on 2023-01-04, for GE HealthCare). Applied as
a split it amounts to selling the shares received and buying the parent with
the proceeds, which keeps the total return right and the share count wrong. Other spin-offs come as a large dividend (Kraft
out of Mondelez, 14.17 on 2012-10-02), which is right too.

**Eleven events since 2000 are wrong at the source**: a dividend and a split
on the same day, for one spin-off counted twice or a reverse split with a
distribution Yahoo misprices (ABT 2004-05-03, CNP 2002-10-01, DHR 2016-07-05,
ETN 2001-01-02, EXPE 2011-12-21, JCI 2007-07-02, TMUS 2013-05-01; the other
four, HST, IRM and SPG twice, are stock dividends and look right). The rows
are stored as Yahoo gives them, and its own adjusted close has the same
error: Danaher gains 39 % on 2016-07-05 where its holders gained 4 %.
`check_bars` reports every such day.

A window is self-contained as long as it runs to today: the splits after a
day of the window are in the window. Every download does.

Universe: the S&P 500 members of today and of the last two years
(`_constituents.sp500_priced`), plus `ETFS`, a universe free of survivorship
bias for a backtest to default to, and the S&P 500 total-return index. **Companies that left the index and were delisted are
not there**: Yahoo drops a delisted ticker with its history, and nobody
gives those prices away. `sp500-membership` restricts the universe at each
date to the names that were in the index then; it cannot bring back the
ones that died, so a backtest on single stocks keeps a survivorship bias.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import List, Tuple

import numpy as np
import pandas as pd

from ._constituents import sp500_priced
from ._yfinance_common import download_daily_ohlcv
from ..core.source import Window

logger = logging.getLogger("data_ingest.equity_raw")

HISTORY_START = "2000-01-01"

#: Broad and sector funds. An index fund is never delisted for having done
#: badly, so a backtest on these has no survivorship bias.
ETFS = (
    "SPY", "QQQ", "IWM", "EFA", "EEM",          # US large, Nasdaq 100, US small, developed, emerging
    "VTI", "URTH",                              # US total market, MSCI World
    "TLT", "IEF", "LQD", "HYG",                 # long and medium Treasuries, IG and HY credit
    "GLD",                                      # gold
    # The eleven SPDR sector funds.
    "XLB", "XLC", "XLE", "XLF", "XLI", "XLK", "XLP", "XLRE", "XLU", "XLV", "XLY",
)

#: The S&P 500 with dividends reinvested: an index, so no event and no volume.
#: SPY above, with its dividends, is the investable version of the same thing.
TOTAL_RETURN_INDEX = "^SP500TR"

PRICES = ["Open", "High", "Low", "Close"]


def universe() -> List[str]:
    """Every ticker asked of Yahoo, sorted."""
    return sorted(set(sp500_priced()) | set(ETFS) | {TOTAL_RETURN_INDEX})


def window_start(window: Window) -> str:
    if window.full or window.start is None:
        return HISTORY_START
    return window.start.isoformat()


@lru_cache(maxsize=2)
def _download(start: str, today: str) -> pd.DataFrame:
    end = (datetime.fromisoformat(today) + timedelta(days=1)).date().isoformat()
    tickers = universe()
    logger.info("Fetching %s tickers with their dividends and splits from %s", len(tickers), start)
    return download_daily_ohlcv(tickers, start, end, actions=True)


def download(window: Window) -> pd.DataFrame:
    """Yahoo's rows from the window's start **to today**, whatever its end
    (see the module docstring), with the split factor `F` of each day.

    Cached for the process: `ingest run` over the three sources downloads
    once. The three DAGs each download their own.
    """
    today = datetime.now(timezone.utc).date().isoformat()
    return with_split_factor(_download(window_start(window), today))


def with_split_factor(frame: pd.DataFrame) -> pd.DataFrame:
    """Adds `F`: the product of the split ratios taking effect strictly after
    each row's date, for its ticker."""
    if frame.empty:
        return frame.assign(F=pd.Series(dtype="float64"))
    frame = frame.sort_values(["Ticker", "Date"]).reset_index(drop=True)
    ratio = frame["Stock Splits"].where(frame["Stock Splits"] > 0, 1.0)
    # Product of the ratios on or after the row, walking each ticker backwards.
    on_or_after = ratio[::-1].groupby(frame["Ticker"][::-1], sort=False).cumprod()[::-1]
    frame["F"] = on_or_after / ratio
    return frame


def bars(frame: pd.DataFrame) -> pd.DataFrame:
    """Open, high, low, close and volume as quoted on the day."""
    quoted = frame[frame[PRICES].notna().any(axis=1)]
    out = pd.DataFrame({"date": quoted["Date"], "ticker": quoted["Ticker"]})
    for column in PRICES:
        out[column.lower()] = quoted[column] * quoted["F"]
    out["volume"] = (quoted["Volume"] / quoted["F"]).round().astype("int64")
    return out.reset_index(drop=True)


def dividends(frame: pd.DataFrame) -> pd.DataFrame:
    paid = frame[frame["Dividends"] > 0]
    return pd.DataFrame(
        {"ticker": paid["Ticker"], "ex_date": paid["Date"], "amount": paid["Dividends"] * paid["F"]}
    ).reset_index(drop=True)


def splits(frame: pd.DataFrame) -> pd.DataFrame:
    split = frame[frame["Stock Splits"] > 0]
    return pd.DataFrame(
        {"ticker": split["Ticker"], "date": split["Date"], "ratio": split["Stock Splits"]}
    ).reset_index(drop=True)


# ───────────────────────────────────────────────────────────── quality ──────

#: A close this far outside the day's range, relative to it, fails. Yahoo's
#: prices are single-precision floats and its high and low are sometimes a
#: cent off the close they are supposed to contain.
RANGE_TOLERANCE = 5e-3

#: A daily total return further than this many robust standard deviations from
#: the ticker's median is reported. The scale is 1.4826 × the median absolute
#: deviation, which a few crashes do not inflate.
OUTLIER_SIGMAS = 30.0
#: Fewer returns than this do not make a scale (a routine run has five): the
#: bound is then absolute.
OUTLIER_MIN_RETURNS = 60
OUTLIER_ABSOLUTE = 0.5

REJECTED = ("non-positive price", "high below low", "close outside range")


def check_bars(quoted: pd.DataFrame, split: pd.DataFrame, dividend: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """The bars fit to store, and a report: one row per failed control, with
    `date`, `ticker`, `check`, `detail` and `stored`.

    A bar that cannot be a quote (a price at or below zero, a high under the
    low, a close outside the day's range) is **not stored**: the consumer
    sees a missing day, never a wrong price. A total return far outside the
    ticker's usual ones is stored and reported: most often a split missing
    from the events, sometimes a real collapse, and the bar alone cannot say
    which. So is a day with both a dividend and a split (see the module
    docstring). Duplicates of a (date, ticker) are reduced to the last one.
    """
    report = []

    duplicated = quoted.duplicated(["date", "ticker"], keep="last")
    report.append(_failures(quoted[duplicated], "duplicate date", "kept the last one", stored=False))
    quoted = quoted[~duplicated]

    prices = quoted[["open", "high", "low", "close"]]
    non_positive = (prices <= 0).any(axis=1)
    inverted = quoted["high"] < quoted["low"]
    slack = RANGE_TOLERANCE * quoted["close"].abs()
    outside = (quoted["close"] > quoted["high"] + slack) | (quoted["close"] < quoted["low"] - slack)
    for mask, check in zip((non_positive, inverted & ~non_positive, outside & ~inverted & ~non_positive), REJECTED):
        failed = quoted[mask]
        detail = [
            f"o={o:.4f} h={h:.4f} l={low:.4f} c={c:.4f}"
            for o, h, low, c in zip(failed["open"], failed["high"], failed["low"], failed["close"])
        ]
        report.append(_failures(failed, check, detail, stored=False))
    kept = quoted[~(non_positive | inverted | outside)].reset_index(drop=True)

    report.append(_return_outliers(kept, split, dividend))
    both = dividend.rename(columns={"ex_date": "date"}).merge(split, on=["ticker", "date"])
    detail = [f"dividend {d:.4f}, split {r:g}" for d, r in zip(both["amount"], both["ratio"])]
    report.append(_failures(both, "dividend and split the same day", detail, stored=True))
    report = pd.concat(report, ignore_index=True)
    return kept, report.sort_values(["ticker", "date"]).reset_index(drop=True)


def _failures(rows: pd.DataFrame, check: str, detail, stored: bool) -> pd.DataFrame:
    return pd.DataFrame(
        {"date": rows["date"], "ticker": rows["ticker"], "check": check, "detail": detail, "stored": stored}
    )


def _return_outliers(quoted: pd.DataFrame, split: pd.DataFrame, dividend: pd.DataFrame) -> pd.DataFrame:
    if quoted.empty:
        return _failures(quoted, "return outlier", "", stored=True)
    frame = (
        quoted.sort_values(["ticker", "date"])
        .merge(split, on=["ticker", "date"], how="left")
        .merge(dividend.rename(columns={"ex_date": "date"}), on=["ticker", "date"], how="left")
    )
    ratio, amount = frame["ratio"].fillna(1.0), frame["amount"].fillna(0.0)
    previous = frame.groupby("ticker")["close"].shift(1)
    frame["r"] = ratio * (frame["close"] + amount) / previous - 1.0
    by_ticker = frame.groupby("ticker")["r"]
    median = by_ticker.transform("median")
    scale = 1.4826 * (frame["r"] - median).abs().groupby(frame["ticker"]).transform("median")
    enough = by_ticker.transform("count") >= OUTLIER_MIN_RETURNS
    bound = np.where(enough & (scale > 0), OUTLIER_SIGMAS * scale, OUTLIER_ABSOLUTE)
    far = frame[(frame["r"] - np.where(enough, median, 0.0)).abs() > bound]
    return _failures(far, "return outlier", [f"return {r:+.1%}" for r in far["r"]], stored=True)


def log_report(source: str, report: pd.DataFrame, limit: int = 50) -> None:
    """One line per failed control, so that nothing is dropped in silence."""
    if report.empty:
        return
    counts = report.groupby(["check", "stored"]).size()
    logger.warning(
        "[%s] %s bars failed a control: %s",
        source, len(report),
        "; ".join(f"{n} {check} ({'stored' if stored else 'not stored'})" for (check, stored), n in counts.items()),
    )
    for row in report.head(limit).itertuples(index=False):
        logger.warning("[%s] %s %s: %s (%s)", source, row.ticker, row.date, row.check, row.detail)
    if len(report) > limit:
        logger.warning("[%s] ... and %s more", source, len(report) - limit)
