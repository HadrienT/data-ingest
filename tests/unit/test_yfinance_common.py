"""The shared yfinance download: prices as quoted, and no row without a quote."""

from datetime import date

import numpy as np
import pandas as pd

from data_ingest.sources import _yfinance_common as yfc


def _multi_ticker_frame():
    """What yf.download(group_by="ticker") returns for a stock and a crypto
    pair over a Friday and a Saturday: the stock is padded with NaN."""
    index = pd.DatetimeIndex(["2026-10-02", "2026-10-03"], name="Date")
    columns = pd.MultiIndex.from_product(
        [["MSFT", "BTC-USD"], ["Open", "High", "Low", "Close", "Adj Close", "Volume"]], names=["Ticker", "Price"]
    )
    frame = pd.DataFrame(np.nan, index=index, columns=columns)
    frame.loc["2026-10-02", "MSFT"] = [519.4, 522.5, 513.7, 517.5, 516.9, 17822300]
    frame.loc["2026-10-02", "BTC-USD"] = [84855.6, 87146.4, 83852.7, 84497.2, 84497.2, 46528418756]
    frame.loc["2026-10-03", "BTC-USD"] = [84503.2, 85019.3, 84429.6, 84763.6, 84763.6, 12574826191]
    return frame


def test_a_day_without_a_quote_is_not_a_row():
    out = yfc._normalize(_multi_ticker_frame(), ["MSFT", "BTC-USD"])
    assert sorted(zip(out["Ticker"], out["Date"])) == [
        ("BTC-USD", date(2026, 10, 2)),
        ("BTC-USD", date(2026, 10, 3)),
        ("MSFT", date(2026, 10, 2)),
    ]
    assert out["Close"].notna().all()
    assert out["Volume"].dtype == "int64"


def test_the_close_is_the_quoted_one_not_the_adjusted_one():
    out = yfc._normalize(_multi_ticker_frame(), ["MSFT", "BTC-USD"]).set_index(["Ticker", "Date"])
    assert out.loc[("MSFT", date(2026, 10, 2)), "Close"] == 517.5
    assert "Adj Close" not in out.columns


def test_a_quote_without_volume_is_kept():
    # An index has prices and no volume.
    frame = _multi_ticker_frame()
    frame.loc["2026-10-02", ("MSFT", "Volume")] = np.nan
    out = yfc._normalize(frame, ["MSFT", "BTC-USD"]).set_index(["Ticker", "Date"])
    assert out.loc[("MSFT", date(2026, 10, 2)), "Volume"] == 0


def test_prices_are_asked_unadjusted(mocker):
    download = mocker.patch.object(yfc.yf, "download", return_value=_multi_ticker_frame())
    yfc.download_daily_ohlcv(["MSFT", "BTC-USD"], "2026-10-02", "2026-10-04")
    assert download.call_args.kwargs["auto_adjust"] is False
