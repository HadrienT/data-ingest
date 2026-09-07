"""How a source describes the table it writes to.

A source declares its shape here rather than writing DDL, so the engine can
create the table, build the upsert statement and generate the revision view
without knowing anything about the domain.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Sequence


class WriteMode(str, Enum):
    """How new rows relate to rows already stored.

    UPSERT suits data that is corrected in place: a price for a given session
    has one true value, and a later fetch replaces an earlier one.

    VERSIONED suits data that is *revised*, which is the normal case for
    fundamentals and macro series: the figure published for Q1 in April is not
    wrong when it is restated in July, it is simply an earlier vintage. Storing
    only the latest value destroys the ability to backtest against what was
    actually knowable at the time, so revisions are kept as separate rows keyed
    by when they were observed.

    APPEND suits immutable events, where nothing is ever restated.
    """

    UPSERT = "upsert"
    VERSIONED = "versioned"
    APPEND = "append"


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    nullable: bool = True

    def ddl(self) -> str:
        return f'"{self.name}" {self.type}{"" if self.nullable else " NOT NULL"}'


@dataclass(frozen=True)
class TableSpec:
    """The table a source writes to.

    `schema` is the domain the data belongs to (prices, macro, fundamentals,
    documents), so related sources sit together and can still be joined.
    """

    schema: str
    name: str
    columns: Sequence[Column]
    primary_key: Sequence[str]
    indexes: Sequence[Sequence[str]] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        names = [c.name for c in self.columns]
        if len(names) != len(set(names)):
            raise ValueError(f"{self.qualified}: duplicate column names")
        for key in self.primary_key:
            if key not in names:
                raise ValueError(f"{self.qualified}: primary key column {key!r} is not declared")
        for index in self.indexes:
            for col in index:
                if col not in names:
                    raise ValueError(f"{self.qualified}: index column {col!r} is not declared")
        if not self.primary_key:
            raise ValueError(f"{self.qualified}: a primary key is required")

    @property
    def qualified(self) -> str:
        return f"{self.schema}.{self.name}"

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    @property
    def nullable_columns(self) -> list[str]:
        """Columns where a missing value must reach Postgres as NULL.

        pandas represents a gap as NaN and psycopg writes that as the IEEE
        value, not as SQL NULL. The two are not interchangeable: NULL is
        skipped by SUM and AVG, whereas a single NaN turns the whole aggregate
        into NaN. The writers convert these before copying.
        """
        return [c.name for c in self.columns if c.nullable]
