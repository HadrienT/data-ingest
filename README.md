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
| `commodity-prices` | `prices.commodity_daily` | upsert | Mon–Fri 23:30 |
| `fred-macro` | `macro.fred_series` | versioned | Mon–Fri 23:00 |
| `fx-rates` | `fx.daily_rates` | upsert | Mon–Fri 23:00 |
| `options-chain-snapshot` | `options.chain_snapshot` | upsert | Mon–Fri 22:30 |
| `dividend-yields` | `prices.dividend_yields` | upsert | Mon–Fri 22:00 |

`fred-macro`'s default series cover the Treasury CMT par-yield curve
(1M → 30Y), T-Bill discount rates, SOFR/Fed Funds, VIX and credit OAS —
between the par yields and the T-Bills it carries everything
`bootstrap_curve()` (quant-modeling) needs to build a discount curve.
`commodity-prices` and `fx-rates` share their download/fetch mechanics with
`sp500-prices` and `fred-macro` respectively (`_yfinance_common.py`,
`_fred_common.py` — leading underscore so the registry does not mistake them
for sources of their own).

`options-chain-snapshot` is different from the other yfinance sources in one
important way: yfinance only ever exposes the *current* option chain, never a
past one, so `--full` and `--since` cannot backfill anything — every run just
fetches today's chain. Running it daily is what builds a history at all; a
calibration that wants a past day's surface reads that day's stored rows back
rather than asking yfinance, which no longer has them. Its ticker universe is
a small fixed list of liquid, options-heavy names (broad ETFs and mega-caps),
not the full `sp500-prices` universe — see the source file for why — and is
overridable with `OPTIONS_CHAIN_TICKERS`.

`dividend-yields` tracks the exact same universe (imported from
`options_chain.py`, not a second list) because it exists to serve the same
consumer: Dupire needs a dividend yield to build a forward, and
`sp500-prices` doesn't carry one.

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
