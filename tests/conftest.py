import pandas as pd
import pytest

from data_ingest.core.spec import Column, TableSpec


@pytest.fixture
def price_spec():
    return TableSpec(
        schema="test_prices",
        name="sample_daily",
        columns=(
            Column("date", "DATE", nullable=False),
            Column("ticker", "TEXT", nullable=False),
            Column("close", "DOUBLE PRECISION"),
            Column("volume", "BIGINT"),
        ),
        primary_key=("date", "ticker"),
        indexes=(("ticker", "date"),),
    )


@pytest.fixture
def revised_spec():
    return TableSpec(
        schema="test_macro",
        name="sample_series",
        columns=(
            Column("series_id", "TEXT", nullable=False),
            Column("date", "DATE", nullable=False),
            Column("value", "DOUBLE PRECISION"),
        ),
        primary_key=("series_id", "date"),
    )


@pytest.fixture
def price_rows():
    return pd.DataFrame(
        {
            "date": [pd.Timestamp("2025-02-07").date()],
            "ticker": ["AAPL"],
            "close": [232.47],
            "volume": [39620300],
        }
    )
