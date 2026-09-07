# data-ingest

Scheduled ingestion of financial and economic data into a local Postgres.

Each dataset is a **source**: a small class declaring the table it fills, how
its rows relate to rows already stored, and when it should run. The engine does
the rest — DDL, staging, writing, scheduling and the command line. Adding a
dataset is a new file in `src/data_ingest/sources/`, not a change to the engine.

It grew out of a single hardcoded S&P 500 pipeline that ran on Google Cloud
(Cloud Scheduler → Cloud Function → BigQuery). That pipeline is now one source
among others, and everything runs on a self-hosted server.

## Sources

| Source | Table | Write mode | Schedule |
|---|---|---|---|
| `sp500-prices` | `prices.sp500_daily` | upsert | Mon–Fri 22:00 |
| `fred-macro` | `macro.fred_series` | versioned | Mon–Fri 23:00 |

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

**`upsert`** — the value for a key has one truth, and a later fetch replaces an
earlier one. Prices work this way: a close for a given session gets corrected,
not revised.

**`versioned`** — the value is *revised*, which is normal for macro series and
fundamentals. An unemployment rate published in January is routinely restated
in February; the January figure is not wrong, it is an earlier vintage.
Overwriting it destroys the ability to ask what was knowable on a given day,
which is exactly what a backtest has to answer. So the engine adds an
`observed_at` column, makes it part of the key, and keeps both rows. A
`<table>_latest` view gives the current vintage for callers that do not care.

A new vintage is written **only when a value actually changes**. Stamping every
run would multiply the table by the run frequency — a daily job over a monthly
series would keep thirty identical copies a month.

**`append`** — immutable events, never restated.

## Adding a source

```python
class MySource(Source):
    name = "my-source"
    description = "What it fetches"
    write_mode = WriteMode.UPSERT
    schedule = "Mon..Fri 20:00"
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

Drop it in `src/data_ingest/sources/`. The registry finds it by import, so
there is no list to update; a list you must remember to update is a list that
goes stale. Re-run `./scripts/install-timer.sh` and it gets its own timer.

`schema` is the domain the data belongs to (`prices`, `macro`, `fundamentals`,
`documents`), so related sources sit together in one database and can still be
joined across domains.

## Running it

Requirements: Docker with the Compose plugin; Python 3.11 to run the tests
outside a container.

```bash
cp .env.example .env
$EDITOR .env                       # PGPASSWORD, and FRED_API_KEY for fred-macro

docker compose up -d postgres
docker compose run --rm ingest run sp500-prices --full   # first backfill
./scripts/install-timer.sh                               # one timer per source
```

Everything binds to `127.0.0.1`; nothing here is reachable off the server.

## Tests

```bash
pip install -e ".[dev]"
pytest -m "not network"    # unit tests plus integration, no upstream API needed
pytest                     # adds the live yfinance and FRED tests
```

Tests marked `postgres` skip themselves when no database is reachable. Tests
marked `network` call live upstream APIs and are excluded in CI, where rate
limits and shifting response shapes make them unreliable.

## A note on verification

When the price history was migrated out of BigQuery, the row count matched the
source exactly — 4,150,962 — while the data was still wrong: a million values
that were NULL upstream had been written as IEEE NaN, which a count cannot see
but which turns every later `SUM` and `AVG` into NaN. Aggregates, null counts
and bounds are compared, not just row counts. `scripts/migrate_from_quant_dev.py`
refuses to declare success on anything less.

## License

MIT. See the LICENSE file.
