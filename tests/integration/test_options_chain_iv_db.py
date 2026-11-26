"""options-chain-iv against a real Postgres: a run computes the stored
snapshot days, writes the quotes, expiries and registry in one transaction,
and a routine run recomputes only the days that changed."""

import dataclasses
import datetime

import pandas as pd
import psycopg
import pytest
from synthetic_options import DAY, spx_chain

from data_ingest.config import dsn
from data_ingest.core.database import Database
from data_ingest.core.runner import run_source
from data_ingest.core.source import Window
from data_ingest.sources import options_chain_iv as iv
from data_ingest.sources.options_chain import OptionsChainSnapshot

pytestmark = pytest.mark.postgres

SCHEMA = "test_options_iv"


class _Chain(OptionsChainSnapshot):
    table = dataclasses.replace(OptionsChainSnapshot.table, schema=SCHEMA)


class _IV(iv.OptionsChainIV):
    table = dataclasses.replace(iv.OptionsChainIV.table, schema=SCHEMA)


@pytest.fixture(scope="module")
def db():
    try:
        with psycopg.connect(dsn(), connect_timeout=3):
            pass
    except Exception as exc:
        pytest.skip(f"No Postgres reachable at {dsn()}: {exc}")
    return Database(dsn())


@pytest.fixture
def clean(db):
    with db.connect() as conn:
        conn.execute(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')
    yield
    with db.connect() as conn:
        conn.execute(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')


def _store_snapshot(db, day=DAY, chain=None):
    chain = (spx_chain() if chain is None else chain).assign(date=day)
    chain["contract_symbol"] = None
    chain["last_trade_date"] = None
    for column in ("last_price", "implied_volatility"):
        chain[column] = None
    chain["volume"] = 0
    chain["open_interest"] = 0
    source = _Chain()
    db.ensure_table(source.table, source.write_mode, companions=source.ensure_companions)
    db.write(source.table, chain, source.write_mode, after_write=source.after_write)


def _count(db, table, where=""):
    with db.connect() as conn:
        return conn.execute(f'SELECT count(*) FROM "{SCHEMA}".{table} {where}').fetchone()[0]


def _loads(db):
    with db.connect() as conn:
        rows = conn.execute(
            f'SELECT ticker, date, loaded_at, row_count, checksum, method_version, curve_source '
            f'FROM "{SCHEMA}".{iv.LOADS_TABLE}'
        ).fetchall()
    return {(r[0], r[1]): r[2:] for r in rows}


def test_a_run_writes_quotes_expiries_and_registry_together(db, clean):
    _store_snapshot(db)
    written = run_source(db, _IV(), Window(start=DAY, end=DAY))

    assert written == _count(db, "chain_iv") > 0
    assert _count(db, iv.EXPIRIES_TABLE) == len(set(map(tuple, spx_chain()[["expiry", "root"]].values)))
    loads = _loads(db)
    assert list(loads) == [("^SPX", DAY)]
    _, row_count, checksum, version, curve = loads[("^SPX", DAY)]
    assert row_count == written and checksum and version == iv.METHOD_VERSION and curve == "parity"


def test_a_routine_run_recomputes_only_the_days_that_changed(db, clean):
    other = DAY - datetime.timedelta(days=1)
    _store_snapshot(db, DAY)
    _store_snapshot(db, other)
    source = _IV()
    window = Window(start=other, end=DAY)
    run_source(db, source, window)
    before = _loads(db)

    with db.connect() as conn:
        assert source.days_to_compute(conn, window) == []
    _store_snapshot(db, DAY)  # a re-run of the snapshot
    with db.connect() as conn:
        assert source.days_to_compute(conn, window) == [DAY]
        assert source.days_to_compute(conn, Window.everything()) == [other, DAY]

    run_source(db, source, window)
    after = _loads(db)
    assert after[("^SPX", other)] == before[("^SPX", other)]
    assert after[("^SPX", DAY)][0] > before[("^SPX", DAY)][0]
    assert after[("^SPX", DAY)][2] == before[("^SPX", DAY)][2]  # same quotes, same checksum


def test_another_method_version_is_recomputed(db, clean, monkeypatch):
    _store_snapshot(db)
    source = _IV()
    run_source(db, source, Window(start=DAY, end=DAY))
    monkeypatch.setattr(iv, "METHOD_VERSION", iv.METHOD_VERSION + 1)
    with db.connect() as conn:
        assert source.days_to_compute(conn, Window(start=DAY, end=DAY)) == [DAY]
    run_source(db, source, Window(start=DAY, end=DAY))
    assert _loads(db)[("^SPX", DAY)][3] == iv.METHOD_VERSION
    assert _count(db, "chain_iv", f"WHERE method_version <> {iv.METHOD_VERSION}") == 0


def test_a_ticker_no_longer_computed_loses_its_rows(db, clean):
    """When a recomputation keeps no quote of a ticker, the earlier rows go:
    the three tables always describe one computation."""
    _store_snapshot(db)
    source = _IV()
    run_source(db, source, Window(start=DAY, end=DAY))
    assert _count(db, "chain_iv") > 0

    # The same day, now with a chain of a single root nobody can settle,
    # next to a second ticker that is still computed.
    spx = spx_chain()
    unknown = spx[spx["root"] == "SPX"].assign(root="SPXZ")
    xsp = spx[spx["root"] == "SPX"].assign(ticker="^XSP")
    _store_snapshot(db, chain=pd.concat([unknown, xsp], ignore_index=True))
    run_source(db, source, Window(start=DAY, end=DAY))

    assert _count(db, "chain_iv", "WHERE ticker = '^SPX'") == 0
    assert _loads(db)[("^SPX", DAY)][1] == 0
    assert _count(db, iv.EXPIRIES_TABLE, "WHERE ticker = '^SPX' AND rejection_reason = 'unknown_root'") > 0
