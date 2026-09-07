"""The engine against a real Postgres.

Skipped when no database is reachable, so the unit suite stays runnable
anywhere. Locally: docker compose up -d postgres && pytest tests/integration
"""

import datetime

import pandas as pd
import psycopg
import pytest

from data_ingest.config import dsn
from data_ingest.core.database import OBSERVED_AT, Database
from data_ingest.core.spec import WriteMode

pytestmark = pytest.mark.postgres


@pytest.fixture(scope="module")
def db():
    try:
        with psycopg.connect(dsn(), connect_timeout=3):
            pass
    except Exception as exc:
        pytest.skip(f"No Postgres reachable at {dsn()}: {exc}")
    return Database(dsn())


@pytest.fixture
def prices(db, price_spec):
    db.ensure_table(price_spec, WriteMode.UPSERT)
    with db.connect() as conn:
        conn.execute(f'TRUNCATE "{price_spec.schema}"."{price_spec.name}"')
    yield price_spec
    with db.connect() as conn:
        conn.execute(f'DROP SCHEMA IF EXISTS "{price_spec.schema}" CASCADE')


@pytest.fixture
def revised(db, revised_spec):
    db.ensure_table(revised_spec, WriteMode.VERSIONED)
    with db.connect() as conn:
        conn.execute(f'TRUNCATE "{revised_spec.schema}"."{revised_spec.name}"')
    yield revised_spec
    with db.connect() as conn:
        conn.execute(f'DROP SCHEMA IF EXISTS "{revised_spec.schema}" CASCADE')


# ------------------------------------------------------------------- schema

def test_ensure_table_is_idempotent(db, prices, price_rows):
    db.write(prices, price_rows, WriteMode.UPSERT)
    db.ensure_table(prices, WriteMode.UPSERT)  # second call must not wipe anything
    assert db.row_count(prices) == 1


def test_index_is_created_from_the_spec(db, prices):
    with db.connect() as conn:
        rows = conn.execute(
            "SELECT indexname FROM pg_indexes WHERE schemaname = %s AND tablename = %s",
            (prices.schema, prices.name),
        ).fetchall()
    assert any("ticker_date" in r[0] for r in rows)


# -------------------------------------------------------------------- write

def test_upsert_replaces_on_conflict(db, prices, price_rows):
    db.write(prices, price_rows, WriteMode.UPSERT)
    corrected = price_rows.copy()
    corrected["close"] = [999.99]
    db.write(prices, corrected, WriteMode.UPSERT)

    with db.connect() as conn:
        value = conn.execute(f'SELECT close FROM "{prices.schema}"."{prices.name}"').fetchone()[0]
    assert db.row_count(prices) == 1
    assert value == pytest.approx(999.99)


def test_missing_values_are_null_not_nan(db, prices, price_rows):
    """NaN would silently poison every later SUM and AVG over the column."""
    gapped = price_rows.copy()
    gapped["close"] = [float("nan")]
    db.write(prices, gapped, WriteMode.UPSERT)

    with db.connect() as conn:
        nulls = conn.execute(
            f'SELECT COUNT(*) FROM "{prices.schema}"."{prices.name}" WHERE close IS NULL'
        ).fetchone()[0]
        nans = conn.execute(
            f"SELECT COUNT(*) FROM \"{prices.schema}\".\"{prices.name}\" WHERE close = 'NaN'::float8"
        ).fetchone()[0]
    assert (nulls, nans) == (1, 0)


def test_rejects_a_frame_missing_a_column(db, prices, price_rows):
    with pytest.raises(ValueError, match="no \\['volume'\\] column"):
        db.write(prices, price_rows.drop(columns=["volume"]), WriteMode.UPSERT)


def test_empty_frame_writes_nothing(db, prices):
    assert db.write(prices, pd.DataFrame(), WriteMode.UPSERT) == 0


# ---------------------------------------------------------------- versioned

def _observation(value, observed_at=None):
    row = {
        "series_id": ["UNRATE"],
        "date": [datetime.date(2025, 1, 1)],
        "value": [value],
    }
    if observed_at is not None:
        row[OBSERVED_AT.name] = [observed_at]
    return pd.DataFrame(row)


def test_versioned_keeps_every_revision(db, revised):
    """A restated figure is a new vintage, not a correction of the old one."""
    db.write(revised, _observation(4.0, pd.Timestamp("2025-02-01", tz="UTC")), WriteMode.VERSIONED)
    db.write(revised, _observation(4.2, pd.Timestamp("2025-03-01", tz="UTC")), WriteMode.VERSIONED)

    assert db.row_count(revised) == 2

    with db.connect() as conn:
        values = conn.execute(
            f'SELECT value FROM "{revised.schema}"."{revised.name}" ORDER BY {OBSERVED_AT.name}'
        ).fetchall()
    assert [v[0] for v in values] == [pytest.approx(4.0), pytest.approx(4.2)]


def test_latest_view_shows_the_current_vintage(db, revised):
    db.write(revised, _observation(4.0, pd.Timestamp("2025-02-01", tz="UTC")), WriteMode.VERSIONED)
    db.write(revised, _observation(4.2, pd.Timestamp("2025-03-01", tz="UTC")), WriteMode.VERSIONED)

    with db.connect() as conn:
        rows = conn.execute(f'SELECT series_id, value FROM "{revised.schema}"."{revised.name}_latest"').fetchall()
    assert rows == [("UNRATE", pytest.approx(4.2))]


def test_versioned_rerun_of_the_same_vintage_is_a_noop(db, revised):
    """Re-running today's fetch must not manufacture a fake revision."""
    stamp = pd.Timestamp("2025-02-01", tz="UTC")
    db.write(revised, _observation(4.0, stamp), WriteMode.VERSIONED)
    db.write(revised, _observation(4.0, stamp), WriteMode.VERSIONED)

    assert db.row_count(revised) == 1


def test_versioned_stamps_observed_at_when_absent(db, revised):
    db.write(revised, _observation(4.0), WriteMode.VERSIONED)
    with db.connect() as conn:
        stamp = conn.execute(
            f'SELECT {OBSERVED_AT.name} FROM "{revised.schema}"."{revised.name}"'
        ).fetchone()[0]
    assert stamp is not None


def test_versioned_preserves_a_supplied_observation_time(db, revised):
    """Backfilling historical vintages must keep their real observation dates.

    Slicing the frame to the source's declared columns used to drop a
    caller-supplied observed_at and restamp it as "now", which collapsed every
    vintage onto the ingestion date.
    """
    stamp = pd.Timestamp("2019-06-15 12:00:00", tz="UTC")
    db.write(revised, _observation(3.7, stamp), WriteMode.VERSIONED)

    with db.connect() as conn:
        stored = conn.execute(
            f'SELECT {OBSERVED_AT.name} FROM "{revised.schema}"."{revised.name}"'
        ).fetchone()[0]
    assert stored == stamp.to_pydatetime()


def test_versioned_ignores_an_unchanged_rerun(db, revised):
    """Re-running when nothing moved upstream must not store a new vintage.

    Stamping every run would multiply the table by the run frequency: a daily
    job over a monthly series would keep thirty identical copies a month. A
    revision is a change, not a snapshot.
    """
    db.write(revised, _observation(4.0, pd.Timestamp("2025-02-01", tz="UTC")), WriteMode.VERSIONED)
    written = db.write(revised, _observation(4.0, pd.Timestamp("2025-02-02", tz="UTC")), WriteMode.VERSIONED)

    assert written == 0
    assert db.row_count(revised) == 1


def test_versioned_records_a_value_appearing(db, revised):
    """NULL becoming a number is a revision, which <> would have missed."""
    db.write(revised, _observation(None, pd.Timestamp("2025-02-01", tz="UTC")), WriteMode.VERSIONED)
    db.write(revised, _observation(4.0, pd.Timestamp("2025-02-02", tz="UTC")), WriteMode.VERSIONED)

    assert db.row_count(revised) == 2
    with db.connect() as conn:
        current = conn.execute(
            f'SELECT value FROM "{revised.schema}"."{revised.name}_latest"'
        ).fetchone()[0]
    assert current == pytest.approx(4.0)


def test_versioned_records_a_genuine_revision(db, revised):
    db.write(revised, _observation(4.0, pd.Timestamp("2025-02-01", tz="UTC")), WriteMode.VERSIONED)
    written = db.write(revised, _observation(4.2, pd.Timestamp("2025-03-01", tz="UTC")), WriteMode.VERSIONED)

    assert written == 1
    assert db.row_count(revised) == 2
