"""The contract every ingestion source implements."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import TYPE_CHECKING, Iterator, Optional, Sequence

import pandas as pd

from .spec import TableSpec, WriteMode

if TYPE_CHECKING:
    import psycopg

    from .database import Database


@dataclass(frozen=True)
class Window:
    """The period a run should fetch.

    `full` asks the source for its entire history, which is how a backfill is
    expressed without every source inventing its own flag.
    """

    start: Optional[date] = None
    end: Optional[date] = None
    full: bool = False

    @classmethod
    def recent(cls, days: int = 5) -> "Window":
        today = datetime.now(timezone.utc).date()
        return cls(start=today - timedelta(days=days), end=today)

    @classmethod
    def everything(cls) -> "Window":
        return cls(full=True)

    def __str__(self) -> str:
        if self.full:
            return "full history"
        return f"{self.start} to {self.end}"


class Source(ABC):
    """One dataset, fetched from one upstream provider.

    Adding a source means writing one of these and dropping it in the sources
    package; the engine handles DDL, staging, writing and scheduling from the
    declarations below.
    """

    #: Stable identifier used on the command line and in the run log.
    name: str

    #: Human-readable description, shown by `ingest list`.
    description: str = ""

    #: The table this source writes to.
    table: TableSpec

    #: How new rows relate to stored ones. See WriteMode.
    write_mode: WriteMode = WriteMode.UPSERT

    #: Cron expression for the routine run, consumed by the Airflow DAG that
    #: `airflow/dags/data_ingest_dags.py` generates for this source.
    schedule: str = "0 22 * * 1-5"

    #: How far back a routine (non-full) run should look. More than one day, so
    #: a missed run or a late upstream correction is picked up by the next one.
    lookback_days: int = 5

    #: The DATE column that says how far the table goes. When set, a routine
    #: run starts at the last date stored if that is older than the lookback:
    #: after a week with the machine off, the missed days are fetched rather
    #: than left as a hole no later run would look at. For sources whose
    #: `fetch` honours `window.start`; None keeps the plain lookback.
    catch_up_on: Optional[str] = None

    #: For a source computed from what other sources stored rather than
    #: fetched from upstream: their names. Its Airflow DAG then runs once
    #: every one of them has run, instead of on `schedule`, and `ingest run
    #: --all` runs it after them.
    inputs: Sequence[str] = ()

    @abstractmethod
    def fetch(self, window: Window) -> pd.DataFrame:
        """Return rows for the window, with columns matching the table spec.

        Returning an empty DataFrame is a normal outcome (a holiday, nothing
        published yet) and must not raise.
        """

    def frames(self, db: "Database", window: Window) -> Iterator[pd.DataFrame]:
        """The frames a run writes, each in a transaction of its own.

        By default the one frame `fetch` returns. A source that reads the
        database (one with `inputs`), or whose history is too large for one
        frame, overrides this and yields its rows in parts, a day at a time
        for instance: a part is written before the next is computed.
        """
        yield self.fetch(window)

    def ensure_companions(self, conn: "psycopg.Connection") -> None:
        """Create the tables this source maintains next to its own.

        Runs in the same transaction as the creation of `table`, on every run.
        Most sources have none.
        """

    def after_write(self, cur: "psycopg.Cursor", frame: pd.DataFrame) -> None:
        """Runs on the write's cursor, after the rows are inserted and before
        the commit, with the frame that was written.

        For state that must never disagree with the table, such as a registry
        of what was loaded: it commits or rolls back with the rows. Most
        sources need nothing here.
        """

    def default_window(self) -> Window:
        return Window.recent(self.lookback_days)

    def __repr__(self) -> str:
        return f"<Source {self.name} -> {self.table.qualified} ({self.write_mode.value})>"
