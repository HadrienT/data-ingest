"""A malformed spec must fail when the source is written, not at write time."""

import pytest

from data_ingest.core.spec import Column, TableSpec, WriteMode


def make(**kwargs):
    base = dict(
        schema="s",
        name="t",
        columns=(Column("a", "TEXT", nullable=False), Column("b", "INT")),
        primary_key=("a",),
    )
    base.update(kwargs)
    return TableSpec(**base)


def test_qualified_name():
    assert make().qualified == "s.t"


def test_rejects_primary_key_that_is_not_a_column():
    with pytest.raises(ValueError, match="not declared"):
        make(primary_key=("missing",))


def test_rejects_index_on_unknown_column():
    with pytest.raises(ValueError, match="not declared"):
        make(indexes=(("nope",),))


def test_rejects_duplicate_columns():
    with pytest.raises(ValueError, match="duplicate"):
        make(columns=(Column("a", "TEXT"), Column("a", "INT")), primary_key=("a",))


def test_requires_a_primary_key():
    with pytest.raises(ValueError, match="primary key is required"):
        make(primary_key=())


def test_nullable_columns_excludes_not_null():
    assert make().nullable_columns == ["b"]


def test_write_modes_are_stable_strings():
    # Persisted in docs and systemd units; renaming one is a breaking change.
    assert [m.value for m in WriteMode] == ["upsert", "versioned", "append", "snapshot"]


def test_snapshot_key_must_be_part_of_the_primary_key():
    with pytest.raises(ValueError, match="snapshot key column 'close'"):
        TableSpec(
            schema="s",
            name="t",
            columns=(Column("date", "DATE"), Column("close", "DOUBLE PRECISION")),
            primary_key=("date",),
            snapshot_key=("close",),
        )
