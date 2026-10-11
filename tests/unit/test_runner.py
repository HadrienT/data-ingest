"""routine_window: the lookback, or everything since the last stored date."""

from datetime import date, timedelta

import pandas as pd
import pytest

from data_ingest.core.runner import routine_window
from data_ingest.core.source import Source, Window
from data_ingest.core.spec import WriteMode


class _Database:
    def __init__(self, last, exists=True):
        self.last, self.exists = last, exists

    def table_exists(self, spec):
        return self.exists

    def bounds(self, spec, column):
        assert column == "date"
        return (date(2000, 1, 3), self.last)


@pytest.fixture
def source(price_spec):
    class Prices(Source):
        name = "prices"
        table = price_spec
        write_mode = WriteMode.UPSERT
        lookback_days = 5
        catch_up_on = "date"

        def fetch(self, window: Window) -> pd.DataFrame:
            return pd.DataFrame()

    return Prices()


def test_a_run_after_a_long_gap_starts_at_the_last_stored_date(source):
    last = source.default_window().start - timedelta(days=40)
    window = routine_window(_Database(last), source)
    assert window.start == last
    assert window.end == source.default_window().end


def test_a_run_on_time_keeps_the_lookback(source):
    window = routine_window(_Database(source.default_window().end), source)
    assert window == source.default_window()


def test_an_empty_or_missing_table_keeps_the_lookback(source):
    assert routine_window(_Database(None), source) == source.default_window()
    assert routine_window(_Database(None, exists=False), source) == source.default_window()


def test_a_source_that_does_not_ask_is_left_alone(source):
    type(source).catch_up_on = None
    assert routine_window(_Database(date(2020, 1, 1)), source) == source.default_window()
