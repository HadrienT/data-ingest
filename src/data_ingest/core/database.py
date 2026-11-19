"""Postgres access: DDL generated from a TableSpec, and the write strategies."""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Callable, Iterator, Optional

import pandas as pd
import psycopg
from psycopg import sql

from .spec import Column, TableSpec, WriteMode

logger = logging.getLogger("data_ingest.database")

#: Runs inside `ensure_table`'s transaction, after the table exists.
Companions = Callable[[psycopg.Connection], None]

#: Runs on the write's cursor after the insert and before the commit, with the
#: frame that was written.
AfterWrite = Callable[[psycopg.Cursor, pd.DataFrame], None]

#: Added by the engine to versioned tables. Sources never declare it.
OBSERVED_AT = Column("observed_at", "TIMESTAMPTZ", nullable=False)


class Database:
    """Owns the connection and everything that touches SQL."""

    def __init__(self, dsn: str):
        self._dsn = dsn

    @contextmanager
    def connect(self) -> Iterator[psycopg.Connection]:
        with psycopg.connect(self._dsn) as conn:
            yield conn

    # ---------------------------------------------------------------- schema

    def _stored_columns(self, spec: TableSpec, mode: WriteMode) -> list[Column]:
        if mode is WriteMode.VERSIONED:
            return [*spec.columns, OBSERVED_AT]
        return list(spec.columns)

    def _stored_primary_key(self, spec: TableSpec, mode: WriteMode) -> list[str]:
        # A revision is not a correction, so the observation time is part of
        # the identity of the row rather than something that overwrites it.
        if mode is WriteMode.VERSIONED:
            return [*spec.primary_key, OBSERVED_AT.name]
        return list(spec.primary_key)

    def ensure_table(
        self,
        spec: TableSpec,
        mode: WriteMode = WriteMode.UPSERT,
        companions: Optional[Companions] = None,
    ) -> None:
        """Creates the schema, table, indexes and, for versioned tables, the
        `<table>_latest` view. Safe to call on every run.

        `companions` creates the tables a source maintains next to its own
        (see `Source.ensure_companions`), in the same transaction.
        """
        columns = self._stored_columns(spec, mode)
        primary_key = self._stored_primary_key(spec, mode)

        column_ddl = ",\n    ".join(c.ddl() for c in columns)
        pk_ddl = ", ".join(f'"{c}"' for c in primary_key)

        with self.connect() as conn:
            conn.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(spec.schema))
            )
            conn.execute(
                sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} (\n    " + column_ddl + ",\n    PRIMARY KEY (" + pk_ddl + ")\n)").format(
                    sql.Identifier(spec.schema), sql.Identifier(spec.name)
                )
            )
            for index in spec.indexes:
                conn.execute(
                    sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {}.{} ({})").format(
                        sql.Identifier(f"{spec.name}_{'_'.join(index)}_idx"),
                        sql.Identifier(spec.schema),
                        sql.Identifier(spec.name),
                        sql.SQL(", ").join(sql.Identifier(c) for c in index),
                    )
                )
            if mode is WriteMode.VERSIONED:
                self._ensure_latest_view(conn, spec)
            if companions is not None:
                companions(conn)

        logger.info("Table %s ready (%s)", spec.qualified, mode.value)

    def _ensure_latest_view(self, conn: psycopg.Connection, spec: TableSpec) -> None:
        """Current vintage of every key, for callers that do not care about history."""
        key = sql.SQL(", ").join(sql.Identifier(c) for c in spec.primary_key)
        conn.execute(
            sql.SQL(
                "CREATE OR REPLACE VIEW {}.{} AS "
                "SELECT DISTINCT ON ({key}) * FROM {}.{} ORDER BY {key}, {observed} DESC"
            ).format(
                sql.Identifier(spec.schema),
                sql.Identifier(f"{spec.name}_latest"),
                sql.Identifier(spec.schema),
                sql.Identifier(spec.name),
                key=key,
                observed=sql.Identifier(OBSERVED_AT.name),
            )
        )

    # ----------------------------------------------------------------- write

    def write(
        self,
        spec: TableSpec,
        df: pd.DataFrame,
        mode: WriteMode = WriteMode.UPSERT,
        after_write: Optional[AfterWrite] = None,
    ) -> int:
        """Writes a frame according to the source's write mode.

        Rows are staged in a temporary table dropped on commit, so a failure
        part way through cannot leave the target half-updated. `after_write`
        (see `Source.after_write`) runs in the same transaction, so whatever it
        maintains commits or rolls back together with the rows.
        """
        if df.empty:
            logger.info("%s: nothing to write", spec.qualified)
            return 0

        frame = _prepare(spec, df, mode)
        columns = self._stored_columns(spec, mode)
        names = [c.name for c in columns]
        primary_key = self._stored_primary_key(spec, mode)

        col_sql = sql.SQL(", ").join(sql.Identifier(n) for n in names)
        target = sql.SQL("{}.{}").format(sql.Identifier(spec.schema), sql.Identifier(spec.name))

        if mode is WriteMode.UPSERT:
            updatable = [n for n in names if n not in primary_key]
            if updatable:
                conflict = sql.SQL("ON CONFLICT ({}) DO UPDATE SET {}").format(
                    sql.SQL(", ").join(sql.Identifier(c) for c in primary_key),
                    sql.SQL(", ").join(
                        sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(n)) for n in updatable
                    ),
                )
            else:
                conflict = sql.SQL("ON CONFLICT DO NOTHING")
        else:
            conflict = sql.SQL("ON CONFLICT DO NOTHING")

        if mode is WriteMode.SNAPSHOT:
            if not spec.snapshot_key:
                raise ValueError(f"{spec.qualified}: the snapshot write mode needs a snapshot_key")
            # Every stored row of a touched snapshot was just deleted, so a
            # conflict can only be a duplicate inside the frame: let it raise.
            conflict = sql.SQL("")

        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("CREATE TEMP TABLE staging (LIKE {} INCLUDING DEFAULTS) ON COMMIT DROP").format(target)
                )
                with cur.copy(sql.SQL("COPY staging ({}) FROM STDIN").format(col_sql)) as copy:
                    for row in frame[names].itertuples(index=False, name=None):
                        copy.write_row(row)

                if mode is WriteMode.SNAPSHOT:
                    cur.execute(self._snapshot_delete(spec, target))
                    replaced = cur.rowcount
                if mode is WriteMode.VERSIONED:
                    statement = self._versioned_insert(spec, target, names, col_sql)
                else:
                    statement = sql.SQL("INSERT INTO {} ({}) SELECT {} FROM staging {}").format(
                        target, col_sql, col_sql, conflict
                    )
                cur.execute(statement)
                written = cur.rowcount
                if after_write is not None:
                    after_write(cur, frame)

        if mode is WriteMode.SNAPSHOT:
            logger.info("%s: %s rows replaced by %s", spec.qualified, replaced, written)
        else:
            logger.info("%s: %s rows sent, %s written", spec.qualified, len(frame), written)
        return written

    def _snapshot_delete(self, spec: TableSpec, target: sql.Composed) -> sql.Composed:
        """Deletes every stored row of the snapshots the staged frame touches,
        so the insert that follows replaces each of them as a whole."""
        key = sql.SQL(", ").join(sql.Identifier(c) for c in spec.snapshot_key)
        match = sql.SQL(" AND ").join(
            sql.SQL("t.{c} = s.{c}").format(c=sql.Identifier(c)) for c in spec.snapshot_key
        )
        return sql.SQL("DELETE FROM {target} t USING (SELECT DISTINCT {key} FROM staging) s WHERE {match}").format(
            target=target, key=key, match=match
        )

    def _versioned_insert(self, spec: TableSpec, target: sql.Composed, names: list[str], col_sql: sql.Composed) -> sql.Composed:
        """Insert only the rows whose values differ from the stored vintage.

        Stamping every run as a new vintage would multiply the table by the run
        frequency: a daily job over a monthly series would store thirty
        identical copies a month. A revision is a *change*, so a row is written
        only when the upstream value actually differs from the latest one held,
        or when the key has never been seen. IS DISTINCT FROM rather than <>,
        so a value appearing or disappearing counts as a change.
        """
        latest = sql.SQL("{}.{}").format(
            sql.Identifier(spec.schema), sql.Identifier(f"{spec.name}_latest")
        )
        value_columns = [n for n in names if n not in spec.primary_key and n != OBSERVED_AT.name]

        key_match = sql.SQL(" AND ").join(
            sql.SQL("s.{c} = l.{c}").format(c=sql.Identifier(c)) for c in spec.primary_key
        )
        changed = sql.SQL(" OR ").join(
            sql.SQL("s.{c} IS DISTINCT FROM l.{c}").format(c=sql.Identifier(c)) for c in value_columns
        ) if value_columns else sql.SQL("FALSE")

        return sql.SQL(
            "INSERT INTO {target} ({cols}) "
            "SELECT {prefixed} FROM staging s "
            "LEFT JOIN {latest} l ON {key_match} "
            "WHERE l.{first_key} IS NULL OR ({changed}) "
            "ON CONFLICT DO NOTHING"
        ).format(
            target=target,
            cols=col_sql,
            prefixed=sql.SQL(", ").join(sql.SQL("s.{}").format(sql.Identifier(n)) for n in names),
            latest=latest,
            key_match=key_match,
            first_key=sql.Identifier(spec.primary_key[0]),
            changed=changed,
        )

    # ------------------------------------------------------------ inspection

    def row_count(self, spec: TableSpec) -> int:
        with self.connect() as conn:
            result = conn.execute(
                sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                    sql.Identifier(spec.schema), sql.Identifier(spec.name)
                )
            )
            return result.fetchone()[0]

    def bounds(self, spec: TableSpec, column: str) -> tuple[Optional[object], Optional[object]]:
        with self.connect() as conn:
            result = conn.execute(
                sql.SQL("SELECT MIN({c}), MAX({c}) FROM {}.{}").format(
                    sql.Identifier(spec.schema), sql.Identifier(spec.name), c=sql.Identifier(column)
                )
            )
            return result.fetchone()

    def table_exists(self, spec: TableSpec) -> bool:
        with self.connect() as conn:
            result = conn.execute("SELECT to_regclass(%s)", (spec.qualified,))
            return result.fetchone()[0] is not None


def _prepare(spec: TableSpec, df: pd.DataFrame, mode: WriteMode) -> pd.DataFrame:
    """Validates the frame against the spec and makes it safe to COPY."""
    missing = set(spec.column_names) - set(df.columns)
    if missing:
        raise ValueError(f"{spec.qualified}: source returned no {sorted(missing)} column")

    frame = df[spec.column_names].copy()

    # NaN is not NULL: psycopg writes the IEEE value, which then poisons every
    # SUM and AVG over the column. Convert before the COPY.
    for name in spec.nullable_columns:
        frame[name] = frame[name].astype(object).where(frame[name].notna(), None)

    if mode is WriteMode.VERSIONED:
        # A source that can fetch historical vintages supplies the real
        # observation time; slicing to the declared columns above would
        # otherwise drop it and restamp everything as "now", collapsing the
        # history the versioned mode exists to preserve.
        if OBSERVED_AT.name in df.columns:
            frame[OBSERVED_AT.name] = df[OBSERVED_AT.name].to_numpy()
        else:
            frame[OBSERVED_AT.name] = pd.Timestamp.now(tz="UTC")

    return frame
