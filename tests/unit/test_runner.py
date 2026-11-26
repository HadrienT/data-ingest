"""routine_window: the lookback, or everything since the last stored date."""

from datetime import date, timedelta

import types

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


# ------------------------------------------------- frames and run order


class _Written:
    """A Database stand-in recording what run_source asks it."""

    def __init__(self):
        self.ensured, self.writes = 0, []

    def ensure_table(self, spec, mode, companions=None):
        self.ensured += 1

    def write(self, spec, frame, mode, after_write=None):
        self.writes.append(len(frame))
        return len(frame)


def _source(name, inputs=(), parts=()):
    class Part(Source):
        table = types.SimpleNamespace(qualified="test.parts")

        def fetch(self, window):
            return pd.DataFrame()

        def frames(self, db, window):
            yield from parts

    Part.name, Part.inputs = name, inputs
    return Part()


def test_run_source_writes_each_frame_in_turn_and_skips_empty_ones():
    from data_ingest.core.runner import run_source

    db = _Written()
    frames = [pd.DataFrame({"a": [1, 2]}), pd.DataFrame(), pd.DataFrame({"a": [3]})]
    assert run_source(db, _source("parts", parts=frames), Window.recent()) == 3
    assert db.ensured == 1 and db.writes == [2, 1]


def test_run_source_with_nothing_creates_nothing():
    from data_ingest.core.runner import run_source

    db = _Written()
    assert run_source(db, _source("empty", parts=[pd.DataFrame()]), Window.recent()) == 0
    assert db.ensured == 0


def test_run_order_puts_each_source_after_its_inputs():
    from data_ingest.core.runner import run_order

    derived = _source("a-derived", inputs=("z-raw", "m-raw"))
    ordered = run_order([derived, _source("m-raw"), _source("z-raw")])
    names = [s.name for s in ordered]
    assert names.index("a-derived") > names.index("z-raw")
    assert names.index("a-derived") > names.index("m-raw")


def test_run_order_rejects_a_cycle():
    from data_ingest.core.runner import run_order

    with pytest.raises(ValueError, match="cycle"):
        run_order([_source("a", inputs=("b",)), _source("b", inputs=("a",))])


def test_the_registered_sources_order_runs_the_implied_vols_after_the_chain():
    from data_ingest.core.runner import run_order
    from data_ingest.registry import all_sources

    names = [s.name for s in run_order(all_sources())]
    assert names.index("options-chain-iv") > names.index("options-chain-snapshot")
