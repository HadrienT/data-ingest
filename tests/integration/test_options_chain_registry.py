"""The option chain's load registry against a real Postgres.

The registry is what quant-modeling reads to know whether a snapshot changed
without re-reading it, so these pin the guarantees docs/sources.md gives:
one row per stored (ticker, date), computed from the stored rows, and written
in the same transaction as the chain.
"""

import dataclasses
import datetime

import pandas as pd
import psycopg
import pytest

from data_ingest.config import dsn
from data_ingest.core.database import Database
from data_ingest.sources.options_chain import REGISTRY_TABLE, OptionsChainSnapshot

pytestmark = pytest.mark.postgres

SCHEMA = "test_options"
DAY = datetime.date(2026, 11, 18)


class _TestChain(OptionsChainSnapshot):
    table = dataclasses.replace(OptionsChainSnapshot.table, schema=SCHEMA)


@pytest.fixture(scope="module")
def db():
    try:
        with psycopg.connect(dsn(), connect_timeout=3):
            pass
    except Exception as exc:
        pytest.skip(f"No Postgres reachable at {dsn()}: {exc}")
    return Database(dsn())


@pytest.fixture
def source(db):
    with db.connect() as conn:
        conn.execute(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')
    chain = _TestChain()
    yield chain
    with db.connect() as conn:
        conn.execute(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')


def _quotes(ticker="SPY", day=DAY, strikes=(500.0, 510.0), bid=1.0):
    rows = []
    for strike in strikes:
        for side in ("call", "put"):
            rows.append({
                "date": day, "ticker": ticker, "expiry": datetime.date(2027, 1, 15),
                "root": ticker, "contract_symbol": None,
                "last_trade_date": datetime.datetime(2026, 11, 18, 20, 59, tzinfo=datetime.timezone.utc),
                "option_type": side, "strike": strike, "bid": bid, "ask": bid + 0.1,
                "last_price": bid + 0.05, "volume": 10, "open_interest": 100,
                "implied_volatility": None,
            })
    return pd.DataFrame(rows)


def _load(db, source, frame):
    db.ensure_table(source.table, source.write_mode, companions=source.ensure_companions)
    return db.write(source.table, frame, source.write_mode, after_write=source.after_write)


def _registry(db):
    with db.connect() as conn:
        rows = conn.execute(
            f'SELECT ticker, date, loaded_at, row_count, checksum FROM "{SCHEMA}"."{REGISTRY_TABLE}" '
            "ORDER BY ticker, date"
        ).fetchall()
    return {(r[0], r[1]): r[2:] for r in rows}


def _chain_rows(db):
    with db.connect() as conn:
        return conn.execute(f'SELECT COUNT(*) FROM "{SCHEMA}".chain_snapshot').fetchone()[0]


def test_a_load_records_one_row_per_ticker_and_date(db, source):
    _load(db, source, pd.concat([_quotes("SPY"), _quotes("QQQ", strikes=(400.0,))]))

    registry = _registry(db)
    assert set(registry) == {("SPY", DAY), ("QQQ", DAY)}
    assert registry[("SPY", DAY)][1] == 4
    assert registry[("QQQ", DAY)][1] == 2


def test_an_identical_rerun_keeps_the_checksum_and_moves_loaded_at(db, source):
    _load(db, source, _quotes())
    before = _registry(db)[("SPY", DAY)]
    _load(db, source, _quotes())
    after = _registry(db)[("SPY", DAY)]

    assert after[2] == before[2]
    assert after[0] > before[0]


def test_a_changed_quote_changes_the_checksum(db, source):
    _load(db, source, _quotes())
    before = _registry(db)[("SPY", DAY)]
    _load(db, source, _quotes(bid=1.5))

    assert _registry(db)[("SPY", DAY)][2] != before[2]


def test_a_rerun_replaces_the_snapshot_and_the_registry_follows(db, source):
    """A contract the re-run no longer lists leaves the snapshot."""
    _load(db, source, _quotes(strikes=(500.0, 510.0)))
    _load(db, source, _quotes(strikes=(500.0,)))

    assert _chain_rows(db) == 2
    assert _registry(db)[("SPY", DAY)][1] == 2


def test_a_ticker_missing_from_the_rerun_keeps_its_snapshot(db, source):
    _load(db, source, pd.concat([_quotes("SPY"), _quotes("QQQ")]))
    qqq = _registry(db)[("QQQ", DAY)]
    _load(db, source, _quotes("SPY", bid=1.5))

    assert _registry(db)[("QQQ", DAY)] == qqq
    assert _chain_rows(db) == 8


def test_a_load_leaves_other_snapshots_alone(db, source):
    _load(db, source, _quotes(day=DAY))
    earlier = _registry(db)[("SPY", DAY)]
    _load(db, source, _quotes(day=DAY + datetime.timedelta(days=1)))

    assert _registry(db)[("SPY", DAY)] == earlier


def test_a_failed_write_rolls_back_the_chain_and_the_registry_together(db, source):
    _load(db, source, _quotes())
    registry_before, rows_before = _registry(db), _chain_rows(db)

    def fail_after_recording(cur, frame):
        source.after_write(cur, frame)
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        db.write(source.table, _quotes(strikes=(500.0, 510.0, 520.0), bid=2.0), source.write_mode,
                 after_write=fail_after_recording)

    assert _registry(db) == registry_before
    assert _chain_rows(db) == rows_before


def test_creating_the_registry_backfills_every_stored_snapshot(db, source):
    db.ensure_table(source.table, source.write_mode)  # the chain as it was before the registry
    db.write(source.table, pd.concat([_quotes(day=DAY), _quotes("QQQ", day=DAY - datetime.timedelta(days=1))]))

    db.ensure_table(source.table, source.write_mode, companions=source.ensure_companions)
    backfilled = _registry(db)
    assert set(backfilled) == {("SPY", DAY), ("QQQ", DAY - datetime.timedelta(days=1))}

    _load(db, source, _quotes(day=DAY))  # what a routine run would have recorded
    assert _registry(db)[("SPY", DAY)][2] == backfilled[("SPY", DAY)][2]


#: The chain as it was created before the contract was kept.
_OLD_CHAIN_DDL = """
CREATE TABLE "{schema}".chain_snapshot (
    date DATE NOT NULL, ticker TEXT NOT NULL, expiry DATE NOT NULL, option_type TEXT NOT NULL,
    strike DOUBLE PRECISION NOT NULL, bid DOUBLE PRECISION, ask DOUBLE PRECISION,
    last_price DOUBLE PRECISION, volume BIGINT, open_interest BIGINT, implied_volatility DOUBLE PRECISION,
    PRIMARY KEY (date, ticker, expiry, option_type, strike)
)"""


def test_an_old_chain_is_migrated_and_its_registry_recomputed(db, source):
    with db.connect() as conn:
        conn.execute(f'CREATE SCHEMA "{SCHEMA}"')
        conn.execute(_OLD_CHAIN_DDL.format(schema=SCHEMA))
        conn.execute(
            f'INSERT INTO "{SCHEMA}".chain_snapshot VALUES '
            "('2026-11-18', 'SPY', '2027-01-15', 'call', 500, 1, 1.1, 1.05, 10, 100, NULL)"
        )
        conn.execute(
            f'CREATE TABLE "{SCHEMA}"."{REGISTRY_TABLE}" (ticker TEXT NOT NULL, date DATE NOT NULL, '
            "loaded_at TIMESTAMPTZ NOT NULL, row_count INTEGER NOT NULL, checksum TEXT NOT NULL, "
            "PRIMARY KEY (ticker, date))"
        )
        conn.execute(f"""INSERT INTO "{SCHEMA}"."{REGISTRY_TABLE}" VALUES ('SPY', '2026-11-18', now(), 1, 'old')""")

    db.ensure_table(source.table, source.write_mode, companions=source.ensure_companions)

    with db.connect() as conn:
        root = conn.execute(f'SELECT root, contract_symbol FROM "{SCHEMA}".chain_snapshot').fetchone()
        key = conn.execute(
            "SELECT array_agg(a.attname::text ORDER BY k.ord) FROM pg_constraint c "
            "CROSS JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS k(attnum, ord) "
            "JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.attnum "
            "WHERE c.conrelid = %s::regclass AND c.contype = 'p'",
            (f"{SCHEMA}.chain_snapshot",),
        ).fetchone()[0]
    assert root == ("SPY", None)
    assert tuple(key) == tuple(source.table.primary_key)
    assert _registry(db)[("SPY", DAY)][2] != "old"

    # A second run finds nothing to migrate and accepts the new rows.
    _load(db, source, _quotes())
    assert _registry(db)[("SPY", DAY)][1] == 4
