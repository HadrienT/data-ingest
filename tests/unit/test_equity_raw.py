"""equity-raw-prices, equity-dividends, equity-splits: Yahoo's split-restated
rows turned back into what was quoted on the day, and the controls a bar must
pass to be stored."""

from datetime import date

import numpy as np
import pandas as pd
import pytest

from data_ingest.core.source import Window
from data_ingest.sources import _equity_raw_common as raw
from data_ingest.sources._constituents import sp500_members
from data_ingest.sources.equity_dividends import EquityDividends
from data_ingest.sources.equity_raw_prices import EquityRawPrices
from data_ingest.sources.equity_splits import EquitySplits

D = [date(2020, 8, 26), date(2020, 8, 27), date(2020, 8, 28), date(2020, 8, 31), date(2020, 9, 1)]

# What was quoted: a dividend of 0.80 on the 27th, a four-for-one split
# taking effect on the 31st.
QUOTED_CLOSE = [500.0, 496.0, 500.0, 128.0, 134.0]
QUOTED_VOLUME = [1000, 1200, 900, 4400, 4000]


def yahoo_frame(ticker="AAPL"):
    """The same five days as Yahoo serves them after the split: prices of the
    first three divided by four, their volumes multiplied, the dividend
    divided."""
    factor = [4.0, 4.0, 4.0, 1.0, 1.0]
    close = [c / f for c, f in zip(QUOTED_CLOSE, factor)]
    return pd.DataFrame(
        {
            "Date": D,
            "Ticker": ticker,
            "Open": close,
            "High": [c * 1.01 for c in close],
            "Low": [c * 0.99 for c in close],
            "Close": close,
            "Volume": [v * f for v, f in zip(QUOTED_VOLUME, factor)],
            "Adj Close": close,
            "Dividends": [0.0, 0.80 / 4, 0.0, 0.0, 0.0],
            "Stock Splits": [0.0, 0.0, 0.0, 4.0, 0.0],
        }
    )


def test_prices_and_volumes_are_those_of_the_day():
    bars = raw.bars(raw.with_split_factor(yahoo_frame()))
    assert bars["close"].tolist() == pytest.approx(QUOTED_CLOSE)
    assert bars["volume"].tolist() == QUOTED_VOLUME
    assert bars["high"].iloc[0] == pytest.approx(505.0)


def test_a_dividend_is_per_share_as_counted_on_its_ex_date():
    dividends = raw.dividends(raw.with_split_factor(yahoo_frame()))
    assert dividends.to_dict("records") == [{"ticker": "AAPL", "ex_date": D[1], "amount": pytest.approx(0.80)}]


def test_a_split_ratio_is_new_shares_per_old_share():
    splits = raw.splits(raw.with_split_factor(yahoo_frame()))
    assert splits.to_dict("records") == [{"ticker": "AAPL", "date": D[3], "ratio": 4.0}]


def test_the_split_of_the_day_does_not_restate_the_day_itself():
    frame = raw.with_split_factor(yahoo_frame())
    assert frame["F"].tolist() == [4.0, 4.0, 4.0, 1.0, 1.0]


def test_two_splits_compound_and_tickers_do_not_mix():
    first = yahoo_frame("AAPL")
    first.loc[4, "Stock Splits"] = 2.0
    frame = raw.with_split_factor(pd.concat([yahoo_frame("MSFT"), first], ignore_index=True))
    assert frame.loc[frame["Ticker"] == "AAPL", "F"].tolist() == [8.0, 8.0, 8.0, 2.0, 1.0]
    assert frame.loc[frame["Ticker"] == "MSFT", "F"].tolist() == [4.0, 4.0, 4.0, 1.0, 1.0]


def test_a_reverse_split_has_a_ratio_below_one():
    frame = yahoo_frame()
    frame["Stock Splits"] = [0.0, 0.0, 0.0, 0.125, 0.0]
    assert raw.with_split_factor(frame)["F"].tolist() == [0.125, 0.125, 0.125, 1.0, 1.0]


def test_the_rebuilt_total_return_is_the_holders():
    """s (P_t + D_t) / P_{t-1} - 1 on the stored rows: the split day is an
    ordinary day, the ex-date earns the dividend."""
    frame = raw.with_split_factor(yahoo_frame())
    bars, dividends, splits = raw.bars(frame), raw.dividends(frame), raw.splits(frame)
    merged = bars.merge(dividends.rename(columns={"ex_date": "date"}), how="left").merge(splits, how="left")
    d, s = merged["amount"].fillna(0.0), merged["ratio"].fillna(1.0)
    returns = (s * (merged["close"] + d) / merged["close"].shift(1) - 1).tolist()
    assert returns[1] == pytest.approx((496.0 + 0.80) / 500.0 - 1)
    assert returns[3] == pytest.approx(4 * 128.0 / 500.0 - 1)
    assert returns[4] == pytest.approx(134.0 / 128.0 - 1)


def test_an_event_on_a_day_without_a_quote_is_kept_and_is_not_a_bar():
    frame = yahoo_frame()
    frame.loc[1, ["Open", "High", "Low", "Close", "Volume"]] = None
    frame = raw.with_split_factor(frame)
    assert D[1] not in raw.bars(frame)["date"].tolist()
    assert raw.dividends(frame)["ex_date"].tolist() == [D[1]]


# ─────────────────────────────────────────────────────────── controls ──────

def _bars(closes, ticker="X", start=date(2024, 1, 1)):
    days = pd.bdate_range(start, periods=len(closes)).date
    return pd.DataFrame(
        {"date": days, "ticker": ticker, "open": closes, "high": [c * 1.01 for c in closes],
         "low": [c * 0.99 for c in closes], "close": closes, "volume": 100}
    )


NO_SPLIT = pd.DataFrame({"ticker": [], "date": [], "ratio": []})
NO_DIVIDEND = pd.DataFrame({"ticker": [], "ex_date": [], "amount": []})


def test_a_clean_frame_passes_whole():
    bars = _bars([100.0, 101.0, 100.5])
    kept, report = raw.check_bars(bars, NO_SPLIT, NO_DIVIDEND)
    assert len(kept) == 3 and report.empty


@pytest.mark.parametrize(
    "column, value, check",
    [
        ("close", 0.0, "non-positive price"),
        ("low", -1.0, "non-positive price"),
        ("high", 50.0, "high below low"),
        ("close", 130.0, "close outside range"),
    ],
)
def test_a_bar_that_cannot_be_a_quote_is_reported_and_not_stored(column, value, check):
    bars = _bars([100.0, 101.0, 100.5])
    bars.loc[1, column] = value
    kept, report = raw.check_bars(bars, NO_SPLIT, NO_DIVIDEND)
    assert kept["date"].tolist() == [bars.loc[0, "date"], bars.loc[2, "date"]]
    assert report[["date", "check", "stored"]].values.tolist() == [[bars.loc[1, "date"], check, False]]


def test_a_close_a_rounding_away_from_the_range_is_a_quote():
    bars = _bars([100.0, 101.0, 100.5])
    bars.loc[1, "high"] = 100.99  # close 101.00
    kept, report = raw.check_bars(bars, NO_SPLIT, NO_DIVIDEND)
    assert len(kept) == 3 and report.empty


def test_a_duplicate_is_reduced_to_the_last_one():
    bars = _bars([100.0, 101.0])
    again = bars.iloc[[1]].assign(close=101.2)
    kept, report = raw.check_bars(pd.concat([bars, again], ignore_index=True), NO_SPLIT, NO_DIVIDEND)
    assert kept["close"].tolist() == [100.0, 101.2]
    assert report["check"].tolist() == ["duplicate date"]


def _quiet_series(n=80):
    """A random walk with 1 % daily moves."""
    returns = np.random.default_rng(7).normal(0.0, 0.01, n)
    return (100.0 * np.cumprod(1 + returns)).tolist()


def test_a_split_missing_from_the_events_is_a_return_outlier_and_stays():
    closes = _quiet_series()
    closes[40:] = [c / 2 for c in closes[40:]]  # two-for-one, no event
    bars = _bars(closes)
    kept, report = raw.check_bars(bars, NO_SPLIT, NO_DIVIDEND)
    assert len(kept) == len(bars)
    assert report[["date", "check", "stored"]].values.tolist() == [[bars.loc[40, "date"], "return outlier", True]]


def test_the_same_drop_with_its_split_is_no_outlier():
    closes = _quiet_series()
    closes[40:] = [c / 2 for c in closes[40:]]
    bars = _bars(closes)
    split = pd.DataFrame({"ticker": ["X"], "date": [bars.loc[40, "date"]], "ratio": [2.0]})
    assert raw.check_bars(bars, split, NO_DIVIDEND)[1].empty


def test_a_large_distribution_with_its_dividend_is_no_outlier():
    closes = _quiet_series()
    closes[40:] = [c - 40.0 for c in closes[40:]]
    bars = _bars(closes)
    dividend = pd.DataFrame({"ticker": ["X"], "ex_date": [bars.loc[40, "date"]], "amount": [40.0]})
    assert raw.check_bars(bars, NO_SPLIT, dividend)[1].empty
    assert raw.check_bars(bars, NO_SPLIT, NO_DIVIDEND)[1]["check"].tolist() == ["return outlier"]


def test_a_routine_window_is_too_short_for_a_scale_and_uses_an_absolute_bound():
    kept, report = raw.check_bars(_bars([100.0, 97.0, 104.0, 40.0, 41.0]), NO_SPLIT, NO_DIVIDEND)
    assert len(kept) == 5
    assert report["check"].tolist() == ["return outlier"]
    assert report["detail"].tolist() == ["return -61.5%"]


def test_a_dividend_and_a_split_the_same_day_are_reported():
    bars = _bars([100.0, 101.0, 100.5])
    day = bars.loc[1, "date"]
    split = pd.DataFrame({"ticker": ["X"], "date": [day], "ratio": [1.0]})
    dividend = pd.DataFrame({"ticker": ["X"], "ex_date": [day], "amount": [0.5]})
    report = raw.check_bars(bars, split, dividend)[1]
    assert report[["date", "check", "stored"]].values.tolist() == [[day, "dividend and split the same day", True]]


# ─────────────────────────────────────────────────────────── universe ──────

def test_the_universe_is_stocks_etfs_and_the_total_return_index():
    universe = raw.universe()
    assert universe == sorted(set(universe))
    assert set(raw.ETFS) <= set(universe) and len(raw.ETFS) == 23
    assert {"AAPL", "BRK-B", "^SP500TR"} <= set(universe)
    assert set(sp500_members()) <= set(universe)
    # The futures and crypto pairs of sp500-prices have no place in a share book.
    assert not [t for t in universe if t.endswith("=F") or t.endswith("-USD")]
    assert [t for t in universe if t.startswith("^")] == ["^SP500TR"]


def test_a_window_starts_where_asked_and_a_backfill_in_2000():
    assert raw.window_start(Window(start=date(2026, 10, 1), end=date(2026, 10, 9))) == "2026-10-01"
    assert raw.window_start(Window.everything()) == "2000-01-01"


def test_the_three_sources_share_one_download(mocker):
    mocker.patch.object(raw, "download", return_value=raw.with_split_factor(yahoo_frame()))
    window = Window(start=D[0], end=D[-1])
    prices, dividends, splits = EquityRawPrices(), EquityDividends(), EquitySplits()
    for source, rows in ((prices, 5), (dividends, 1), (splits, 1)):
        frame = source.fetch(window)
        assert len(frame) == rows
        assert list(frame.columns) == source.table.column_names
    assert prices.catch_up_on == "date" and dividends.catch_up_on == "ex_date"


@pytest.mark.network
def test_apple_closed_at_111_94_on_the_first_session_of_2000():
    frame = raw.with_split_factor(
        raw.download_daily_ohlcv(["AAPL"], "2000-01-03", date.today().isoformat(), actions=True)
    )
    first = raw.bars(frame).iloc[0]
    assert first["date"] == date(2000, 1, 3)
    assert first["close"] == pytest.approx(111.9375, abs=1e-3)
