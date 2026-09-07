"""The contract every ingestion source implements."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import pandas as pd

from .spec import TableSpec, WriteMode


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

    #: systemd OnCalendar expression for the routine run.
    schedule: str = "Mon..Fri 22:00"

    #: How far back a routine (non-full) run should look. More than one day, so
    #: a missed run or a late upstream correction is picked up by the next one.
    lookback_days: int = 5

    @abstractmethod
    def fetch(self, window: Window) -> pd.DataFrame:
        """Return rows for the window, with columns matching the table spec.

        Returning an empty DataFrame is a normal outcome (a holiday, nothing
        published yet) and must not raise.
        """

    def default_window(self) -> Window:
        return Window.recent(self.lookback_days)

    def __repr__(self) -> str:
        return f"<Source {self.name} -> {self.table.qualified} ({self.write_mode.value})>"
