# data-ingest

Scheduled ingestion of financial and economic data into a self-hosted Postgres.

[![CI](https://github.com/HadrienT/data-ingest/actions/workflows/ci.yml/badge.svg)](https://github.com/HadrienT/data-ingest/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
![Python 3.11](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-LocalExecutor-017CEE?logo=apacheairflow&logoColor=white)

Each dataset is a **source**: a small class declaring the table it fills, how
its rows relate to rows already stored, and when it should run. The engine does
the rest: DDL, staging, writing, scheduling and the command line. Adding a
dataset is a new file in `src/data_ingest/sources/`, not a change to the engine.

It feeds [quant-modeling](https://github.com/HadrienT/quant-modeling),
[quant-backtest](https://github.com/HadrienT/quant-backtest) and
[quant-microstructure](https://github.com/HadrienT/quant-microstructure). It
grew out of a single hardcoded S&P 500 pipeline on Google Cloud, which is now
one source among 23.

## What it collects

| Domain | Data | Origin |
|---|---|---|
| US equities | Daily prices, raw prices with dividends and splits kept apart, S&P 500 membership since 1996 | Yahoo Finance |
| International equities | CAC 40, DAX, FTSE 100 and Nikkei 225: constituents and indices since 2000 | Yahoo Finance |
| Options | Daily chain snapshots on liquid names, dividend yields | Yahoo Finance |
| Rates and macro | Treasury curve, SOFR, VIX, credit spreads by rating; EUR, GBP, CHF and JPY rates and government curves | FRED, ECB, Bank of England, SNB, Bank of Japan, Japan MoF |
| Traded derivatives | Swaptions, par swap curves, FX options | DTCC public dissemination |
| FX and commodities | ECB reference rates since 1999, USD pairs, commodity prices | ECB, FRED, Yahoo Finance |
| Fundamentals | About 60 XBRL concepts from every 10-K and 10-Q, filing index | SEC EDGAR |
| Credit | One-year default rates per rating category | ESMA CEREP |
| Factors | Fama-French three and five factors, momentum | Kenneth French data library |
| Microstructure | Catalogue of captured order book files, one-minute bars | This repository's capture |

[`docs/sources.md`](docs/sources.md) lists every source with its table and
schedule, and what a consumer must know about each one.

```bash
ingest list                  # what exists
ingest run sp500-prices      # routine run over the source's lookback window
ingest run fred-macro --full # backfill the whole history
ingest run --all             # every source; one failure does not stop the rest
ingest status                # rows and date range per source
```

## Write modes

The mode is the interesting part of a source's declaration, because different
data changes in different ways.

| Mode | Meaning | Example |
|---|---|---|
| `upsert` | A key has one truth; a later fetch replaces an earlier one | A close that gets corrected |
| `versioned` | Values are revised, and earlier vintages are kept | Macro series |
| `append` | Immutable events, never restated | |

`versioned` is what makes a backtest honest. An unemployment rate published in
January is routinely restated in February, and overwriting it destroys the
ability to ask what was knowable on a given day. The engine adds an
`observed_at` column to the key, writes a new row **only when a value actually
changes**, and exposes the current vintage through a `<table>_latest` view.

## Adding a source

```python
class MySource(Source):
    name = "my-source"
    description = "What it fetches"
    write_mode = WriteMode.UPSERT
    schedule = "0 20 * * 1-5"
    lookback_days = 5

    table = TableSpec(
        schema="prices",
        name="my_table",
        columns=(
            Column("date", "DATE", nullable=False),
            Column("instrument", "TEXT", nullable=False),
            Column("value", "DOUBLE PRECISION"),
        ),
        primary_key=("date", "instrument"),
        indexes=(("instrument", "date"),),
    )

    def fetch(self, window: Window) -> pd.DataFrame:
        ...  # return a frame with the declared columns
```

Drop it in `src/data_ingest/sources/`. The registry finds it by import, and
Airflow generates its DAG on the next scan; it starts paused. `schema` is the
domain the data belongs to (`prices`, `macro`, `fundamentals`, `documents`), so
related sources sit together in one database and can still be joined across
domains.

## Order book capture

One service is not a scheduled run. `ingest capture` keeps a connection open to
Bitstamp's public order-by-order feed and records every order created, changed
and deleted into daily binary files, for `quant-microstructure` to replay. The
stream is checked as it is recorded, and `ingest capture-verify` replays a day
and compares it with the exchange's own snapshots. Format, quality checks and
retention are in [`docs/order-book-capture.md`](docs/order-book-capture.md).

## Running it

Requirements: Docker with the Compose plugin; Python 3.11 to run the tests
outside a container.

```bash
cp .env.example .env
$EDITOR .env    # PGPASSWORD, FRED_API_KEY, and the Airflow secrets (see the
                 # generation commands next to each one in .env.example)

docker compose up -d postgres
docker compose run --rm ingest run sp500-prices --full   # first backfill

# once, BEFORE the first `up`: Airflow runs as AIRFLOW_UID and writes into
# these bind mounts; left to Docker they are created root-owned, and the
# dag-processor crash-loops on "No such file or directory" for its logs.
mkdir -p airflow/logs airflow/plugins airflow/config
docker compose up -d airflow-init   # once: migrates Airflow's metadata DB
docker compose up -d                # scheduler, api-server, dag-processor, triggerer

# the order book capture: its directory must exist and be yours first, for
# the same reason (it runs as CAPTURE_UID)
mkdir -p data/capture
docker compose up -d capture
```

The Airflow UI is at `http://127.0.0.1:8080` (login: `AIRFLOW_ADMIN_USER` /
`AIRFLOW_ADMIN_PASSWORD` from `.env`). DAGs start paused; unpause the ones you
want to run on their schedule. Everything binds to `127.0.0.1`.

Scheduling, automatic deployment and rollback are in
[`docs/operations.md`](docs/operations.md).

## Tests

```bash
pip install -e ".[dev]"
pytest -m "not network"    # unit tests plus integration, no upstream API needed
pytest                     # adds the live yfinance, FRED and Bitstamp tests
```

Tests marked `postgres` skip themselves when no database is reachable. Tests
marked `network` call live upstream APIs and are excluded in CI.

## A note on verification

When the price history was migrated out of BigQuery, the row count matched the
source exactly (4,150,962) while the data was still wrong: a million values
that were NULL upstream had been written as IEEE NaN, which a count cannot see
but which turns every later `SUM` and `AVG` into NaN. Aggregates, null counts
and bounds are compared, not just row counts.
`scripts/migrate_from_quant_dev.py` refuses to declare success on anything less.

## License

[MIT](LICENSE)
