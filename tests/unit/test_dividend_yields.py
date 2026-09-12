from data_ingest.sources.dividend_yields import DividendYields
from data_ingest.sources.options_chain import DEFAULT_TICKERS


def test_tracks_the_same_universe_as_options_chain_snapshot():
    # By construction (import, not a duplicated list) -- this source exists
    # to serve the vol-surface pipeline's need for a dividend yield on
    # exactly the tickers it already builds an option chain for.
    assert DividendYields().tickers() == list(DEFAULT_TICKERS)


def test_table_shape():
    table = DividendYields.table
    assert table.qualified == "prices.dividend_yields"
    assert table.primary_key == ("date", "ticker")
