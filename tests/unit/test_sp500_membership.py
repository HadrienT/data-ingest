"""sp500-membership: the committed history is well-formed, and the universe
at a past date holds nobody who entered the index later."""

from datetime import date

import pytest

from data_ingest.core.source import Window
from data_ingest.sources._constituents import sp500_members, sp500_priced
from data_ingest.sources.sp500_prices import SP500Prices
from data_ingest.sources.sp500_membership import SP500Membership, load, members_on

HISTORY_START = date(1996, 1, 2)


@pytest.fixture(scope="module")
def history():
    return load()


def test_every_stay_is_dated_and_ends_after_it_starts(history):
    assert history["date_added"].notna().all() and (history["date_added"] >= HISTORY_START).all()
    closed = history[history["date_removed"].notna()]
    assert (closed["date_removed"] > closed["date_added"]).all()
    assert not history.duplicated(["ticker", "date_added"]).any()


def test_the_stays_of_a_ticker_do_not_overlap(history):
    for _, stays in history.groupby("ticker"):
        stays = stays.sort_values("date_added")
        ends = stays["date_removed"].tolist()
        assert all(end is not None for end in ends[:-1])
        assert all(end <= start for end, start in zip(ends[:-1], stays["date_added"].tolist()[1:]))


@pytest.mark.parametrize("day", [date(1996, 1, 2), date(2000, 3, 10), date(2008, 9, 15), date(2015, 6, 30), date(2024, 1, 2)])
def test_the_index_has_about_five_hundred_members_at_any_date(history, day):
    assert 480 <= len(members_on(history, day)) <= 510


def test_nobody_is_in_the_universe_before_entering_the_index(history):
    """The acceptance criterion: for every stay and at several dates before
    it starts, the ticker is not a member unless an earlier stay says so."""
    for day in (date(2000, 1, 3), date(2005, 6, 1), date(2012, 12, 31), date(2020, 12, 18), date(2026, 1, 2)):
        members = members_on(history, day)
        entered_later = set(history.loc[history["date_added"] > day, "ticker"])
        earlier_stay = set(history.loc[history["date_added"] <= day, "ticker"])
        assert not (members & (entered_later - earlier_stay))


def test_known_entries_exits_and_changes_of_symbol(history):
    assert "TSLA" not in members_on(history, date(2020, 12, 18))
    assert "TSLA" in members_on(history, date(2020, 12, 21))
    # Facebook became META on 2022-06-09: the symbol of the time.
    assert "FB" in members_on(history, date(2020, 1, 2)) and "META" not in members_on(history, date(2020, 1, 2))
    assert "META" in members_on(history, date(2023, 1, 3)) and "FB" not in members_on(history, date(2023, 1, 3))
    # Lehman left the index in September 2008; the file knows it by the
    # symbol it took once delisted.
    assert "LEHMQ" in members_on(history, date(2008, 9, 1)) and "LEHMQ" not in members_on(history, date(2008, 10, 1))
    # A share class is written the way Yahoo writes it.
    assert "BRK-B" in members_on(history, date(2024, 1, 2))


def test_the_removal_day_is_the_first_day_out(history):
    stay = history[(history["ticker"] == "FB")].iloc[0]
    assert stay["ticker"] in members_on(history, date(2022, 6, 8))
    assert stay["date_removed"] == date(2022, 6, 9)
    assert "FB" not in members_on(history, stay["date_removed"])


def test_source_contract(history):
    source = SP500Membership()
    frame = source.fetch(Window.recent(0))
    assert list(frame.columns) == source.table.column_names
    assert len(frame) == len(history) > 1200
    assert frame["as_of"].nunique() == 1


def test_the_priced_universe_is_the_members_and_those_who_left_recently(history):
    today = date(2026, 10, 11)
    priced = set(sp500_priced(today))
    assert set(sp500_members()) <= priced
    # Molson Coors left on 2026-09-21: still priced. Lehman is long gone.
    assert "TAP" in priced and "TAP" not in sp500_members()
    assert "LEHMQ" not in priced
    # Two years on, it is dropped.
    assert "TAP" not in sp500_priced(date(2028, 10, 1))


def test_sp500_prices_follows_the_index_and_keeps_its_extras():
    tickers = SP500Prices().tickers()
    assert tickers == sorted(set(tickers))
    assert set(sp500_members()) <= set(tickers)
    assert {"^GSPC", "SPY", "CL=F", "BTC-USD"} <= set(tickers)
