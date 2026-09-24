"""Sources must be discovered without any manual registration."""

from data_ingest.core.source import Source
from data_ingest.registry import all_sources, get_source

import pytest


def test_discovers_the_shipped_sources():
    names = {s.name for s in all_sources()}
    assert {
        "sp500-prices", "fred-macro", "fx-rates", "commodity-prices",
        "options-chain-snapshot", "dividend-yields", "intl-rates",
        "equity-universe", "intl-equity-prices",
    } <= names


def test_underscore_prefixed_modules_are_not_sources():
    # _fred_common and _yfinance_common hold logic shared between sources,
    # not sources themselves; the registry must skip them by name.
    names = {s.name for s in all_sources()}
    assert "_fred_common" not in names
    assert "_yfinance_common" not in names
    assert "_intl_providers" not in names
    assert "_constituents" not in names


def test_every_source_is_usable():
    for source in all_sources():
        assert isinstance(source, Source)
        assert source.name and source.table and source.schedule
        assert source.table.primary_key


def test_get_source_by_name():
    assert get_source("sp500-prices").table.qualified == "prices.sp500_daily"


def test_unknown_source_lists_what_exists():
    with pytest.raises(KeyError, match="sp500-prices"):
        get_source("does-not-exist")
