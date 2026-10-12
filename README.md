# data-ingest

Scheduled ingestion of financial and economic data into a local Postgres.

Each dataset is a **source**: a small class declaring the table it fills, how
its rows relate to rows already stored, and when it should run. The engine does
the rest — DDL, staging, writing, scheduling and the command line. Adding a
dataset is a new file in `src/data_ingest/sources/`, not a change to the engine.

It grew out of a single hardcoded S&P 500 pipeline that ran on Google Cloud
(Cloud Scheduler → Cloud Function → BigQuery). That pipeline is now one source
among others, and everything runs on a self-hosted server.

One thing here is not a scheduled run: the [order book capture](#order-book-capture),
a connection that stays open and records a crypto exchange's order book into
files, for `quant-microstructure` to replay.

## Sources

| Source | Table | Write mode | Schedule (cron, UTC) |
|---|---|---|---|
| `sp500-prices` | `prices.sp500_daily` | upsert | `0 22 * * 1-5` |
| `commodity-prices` | `prices.commodity_daily` | upsert | `30 23 * * 1-5` |
| `fred-macro` | `macro.fred_series` | versioned | `0 23 * * 1-5` |
| `fx-rates` | `fx.daily_rates` | upsert | `0 23 * * 1-5` |
| `options-chain-snapshot` | `options.chain_snapshot` | upsert | `30 22 * * 1-5` |
| `dividend-yields` | `prices.dividend_yields` | upsert | `0 22 * * 1-5` |
| `intl-rates` | `macro.intl_rates` | upsert | `30 21 * * 1-5` |
| `equity-universe` | `prices.equity_universe` | upsert | `0 6 * * 1` |
| `intl-equity-prices` | `prices.intl_equity_daily` | upsert | `30 18 * * 1-5` |
| `ecb-fx` | `fx.ecb_reference_rates` | upsert | `0 16 * * 1-5` |
| `sec-fundamentals` | `fundamentals.sec_facts` | upsert | `0 5 * * 6` |
| `sec-filings` | `fundamentals.sec_filings` | upsert | `30 4 * * 6` |
| `dtcc-swaptions` | `rates.dtcc_swaptions` | upsert | `30 5 * * *` |
| `dtcc-swap-rates` | `rates.dtcc_swap_rates` | upsert | `35 5 * * *` |
| `dtcc-fx-options` | `fx.dtcc_options` | upsert | `40 5 * * *` |
| `rating-default-rates` | `credit.rating_default_rates` | upsert | `0 7 2 * *` |
| `equity-raw-prices` | `prices.raw_daily` | upsert | `15 22 * * 1-5` |
| `equity-dividends` | `prices.dividends` | upsert | `20 22 * * 1-5` |
| `equity-splits` | `prices.splits` | upsert | `25 22 * * 1-5` |
| `sp500-membership` | `prices.sp500_membership` | upsert | `10 6 * * 1` |
| `french-factors` | `factors.french_daily` | versioned | `0 9 * * 6` |
| `capture-catalogue` | `microstructure.capture_files` | upsert | `*/15 * * * *` |
| `microstructure-bars` | `microstructure.bars_1m` | upsert | `7 * * * *` |

`fred-macro`'s default series cover the Treasury CMT par-yield curve
(1M → 30Y), T-Bill discount rates, SOFR/Fed Funds, VIX, Moody's Aaa/Baa
yields and the ICE BofA credit OAS — aggregate, by rating (AAA → CCC) and, for
investment grade, by maturity bucket (1-3Y → 15Y+), the only free credit term
structure (ICE lets FRED serve three years of history) —
between the par yields and the T-Bills it carries everything
`bootstrap_curve()` (quant-modeling) needs to build a discount curve.
`commodity-prices` and `fx-rates` share their download/fetch mechanics with
`sp500-prices` and `fred-macro` respectively (`_yfinance_common.py`,
`_fred_common.py` — leading underscore so the registry does not mistake them
for sources of their own). The yfinance price sources store Yahoo's "Close":
restated for splits, **not adjusted for dividends**, and no row at all for a
day without a quote.

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

`ecb-fx` stores the ECB's euro reference rates for every currency it
publishes, daily since 1999: `value` is the number of units of `currency` per
euro, and any cross rate follows by division (USD/JPY = JPY per EUR ÷ USD per
EUR). It complements `fx-rates` (FRED H.10, USD pairs at noon New York).

`intl-equity-prices` extends `sp500-prices` to the CAC 40, DAX, FTSE 100 and
Nikkei 225 — every constituent and the index itself, daily OHLCV from yfinance
since 2000. Prices are stored in the line's ISO currency, per row: London
quotes in pence (GBp) are divided by 100, and the few FTSE lines quoted in USD
or EUR stay so. Yahoo publishes no option chains outside the US, so these
markets have prices only. `equity-universe` says which ticker is in which
market (S&P 500 included), with names and currency; a company that leaves an
index keeps its row with an older `as_of`, so the current composition is each
market's latest `as_of`. The lists are committed files
(`sources/data/constituents/`), regenerated by
`scripts/refresh_index_constituents.py` from Wikipedia when an index
rebalances — a reviewed diff rather than a scrape at every run.

`intl-rates` is the non-USD counterpart of `fred-macro`'s rates: EUR, GBP,
CHF and JPY reference rates and government curves, straight from the central
banks and treasuries that publish them for free, with no API key — the ECB
(€STR and its compounded averages, the deposit rate, monthly Euribor averages,
the euro-area AAA zero-coupon curve 3M → 30Y), the Bank of England (SONIA,
Bank Rate, nominal gilt par yields 5/10/20Y), the SNB (SARON and its compound
rates), the Bank of Japan (TONA) and Japan's Ministry of Finance (JGB par
yields 1Y → 40Y). One long table, `(series_id, date, value)` in percent — the
shape of `macro.fred_series` — with ids like `EUR.ESTR` or `JPY.JGB_10Y`; the
catalogue, with each series' kind and tenor, is `CATALOG` in the source file.
What is *not* free is not there: OIS swap curves in every currency, and daily
Euribor fixings (an EMMI licence). The SNB stopped publishing Confederation
bond yields in 2025, so CHF has no government curve. Each provider is fetched
independently; one being down costs only its own series.

`sec-fundamentals` and `sec-filings` read SEC EDGAR for the S&P 500 (no key,
but `SEC_USER_AGENT` must name you with a contact address, or EDGAR answers
403). The first stores ~60 chosen XBRL concepts from every 10-K and 10-Q —
income statement, balance sheet, debt, cash flows — under their raw XBRL
names, one row per fact *and filing*: a 10-K restates prior years, and keeping
each filing's figure with its `filed` date keeps the table point-in-time.
Which tag means "revenue" for a given company is left to the consumer. The
second lists the filings themselves, with the URL of each document, the
company name and its SIC industry.

`dtcc-swaptions` and `dtcc-swap-rates` read the interest-rate file that
DTCC's swap data repository publishes every day under the CFTC's public
dissemination rule: every swap and option traded by a US person, with its
rates, dates, notional and premium, and no counterparty. It is the only free
source of *traded* interest-rate option prices — there is no free swaption
volatility surface, but there are the trades one is built from. The first
source stores the European swaptions of every currency as published (about
1,500 messages a day, half of them on USD SOFR swaps); premium over notional,
inverted through Bachelier's formula, is an implied volatility. The second
stores par swap curves, 1Y to 30Y — SOFR, and for the euro €STR and Euribor,
the swap EUR swaptions are written on: the median fixed rate of the day's
spot-starting swaps per tenor, with its quartiles and the number of trades —
the curve a swaption's forward rate has to be read on, and the swap curve
FRED does not have. The rows are raw; the rules a consumer needs (new trades
only, capped notionals, platform straddles that carry the premium of both
legs) are in the swaption source's docstring. **The files stay downloadable
about two years**, then move to cold storage: `--full` backfills that much,
and a day not ingested in time is gone.

`dtcc-fx-options` reads the foreign-exchange file of the same repository:
the vanilla options of every currency pair, about 5,000 messages a day, a
quarter of them on EUR/USD, with strike, expiry, the two amounts exchanged at
exercise and the premium. Implied FX volatilities are not published for free;
premium over notional, inverted through Garman-Kohlhagen's formula, is one
(6 to 7 % on EUR/USD in October 2026). Rows are raw, and the rules are in the
source's docstring: new trades only, capped amounts, one trade reported
several times, the side the option type refers to. Most trades expire within
three months and few beyond a year, so a long-dated volatility is an
extrapolation. Barrier, digital and non-deliverable options are left out.
A file is 3 MB and 70,000 rows: **backfill with `--since`, a few months at a
time**, rather than `--full`, which holds two years of rows in memory at once.

`rating-default-rates` stores the historical one-year default rate of each
rating category — for S&P, Moody's and Fitch, per calendar year: how many of
the corporate issuers rated `BB` on 1 January defaulted during the year, and
that count as a percentage. It is what a regulatory capital formula calls a
probability of default, as opposed to the one a credit spread implies (which
also pays for bearing the risk, and is several times larger). The agencies'
own default studies are the usual reference, but their terms forbid storing
or redistributing the tables; the same statistics, in a regulated format, are
reported by each agency to ESMA, which publishes them in its central
repository (CEREP) and allows reproduction with acknowledgement. S&P's
figures there start in 2000 and, averaged, match its own long-run study to a
few basis points. Rows are as published — each agency's own scale, percent —
and a year an agency did not report is absent rather than zero; what a
consumer must know (the cohort is the agency's EU entity, no denominator is
published) is in the source's docstring. `--full` takes about twenty minutes:
the repository answers one query in ten seconds.

`equity-raw-prices`, `equity-dividends` and `equity-splits` are what a
backtest reads ([quant-backtest](https://github.com/HadrienT/quant-backtest)
keeps a book in shares and cash). The first stores daily bars **as quoted on
the day, adjusted for nothing**: Apple closed at 111.94 on 2000-01-03, which
Yahoo serves as 0.9994 after four splits. The other two store the events
apart: the dividend per share as counted on its ex-date, and the split ratio
as new shares per old share. A stored row never changes when a later split
happens. With `P` the close, `D` the dividend and `s` the ratio of the day,
the total return is `s (P_t + D_t) / P_{t-1} - 1`;
`scripts/verify_raw_prices.py` checks the stored rows against a fresh
download. A bar that cannot be a quote (a price at or below zero, a high
under the low, a close outside the day's range) is not stored and the run's
log says so; a day without a quote has no row. The universe is the S&P 500,
23 broad and sector ETFs (SPY, QQQ, IWM, VTI, URTH, EFA, EEM, TLT, IEF, LQD,
HYG, GLD and the eleven SPDR sector funds) and `^SP500TR`. **The total-return benchmark is
SPY with its dividends**; the index is there to compare with. What Yahoo gets
wrong, spin-offs above all, is in `_equity_raw_common.py`.

`sp500-membership` is the history of the index since 1996: one row per stay
of a ticker, with the day it entered and the day it left, so that the
universe of a backtest on a past date is the membership on that date and not
today's. The same file is the universe of every S&P 500 source: today's
members for the SEC sources, and for the price sources those that left in
the last two years as well, so that nobody has a ticker list to keep. The file is committed (`sources/data/sp500_membership.csv`) and
regenerated by `scripts/refresh_sp500_membership.py`. **It does not remove
the survivorship bias**: Yahoo has no prices for a delisted company, so 531
of the 1,214 tickers that were ever in the index have prices, and 186 of the
491 members of January 2000. The ETFs have none.

`french-factors` stores the daily Fama-French factors from Kenneth French's
data library: the three-factor set since 1926, the five-factor set since
1963, momentum, and the risk-free rate, **in percent, as published**. The
library revises its history at each CRSP vintage, so the table is versioned
like `fred-macro`; `factors.french_daily_latest` is the current vintage.
`SMB` differs between the two sets: take the factors of one regression from
one `dataset`.

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

Drop it in `src/data_ingest/sources/`. The registry finds it by import, so
there is no list to update; a list you must remember to update is a list that
goes stale. Its DAG appears in the Airflow UI on the next dag-processor scan
(a few seconds) — nothing else to run. It starts paused; unpause it there.

`schema` is the domain the data belongs to (`prices`, `macro`, `fundamentals`,
`documents`), so related sources sit together in one database and can still be
joined across domains.

## Scheduling

Airflow is the scheduler — `airflow/dags/data_ingest_dags.py` generates one
DAG per registered source at parse time (`ingest_<source>`, e.g.
`ingest_sp500_prices`), reading each source's `schedule` straight off the
registry, the same "nothing to hand-register" reasoning the engine already
applies everywhere else. A DAG's single task calls `run_source`
(`core/runner.py`) directly, in-process — the same function the CLI's `ingest
run` uses, not a shelled-out copy of it, so there's exactly one place that
knows what "running a source" means.

It runs as `LocalExecutor`: a scheduler, api-server, dag-processor and
triggerer, no Celery/Redis — the workload is a handful of daily batch runs on
one host, and distributing that across workers would be overhead for
identical throughput. Airflow's own metadata lives in a Postgres separate
from the data store (`airflow-postgres`), so its schema migrations can never
touch ingested data.

Retries (3, 2 minutes apart) and a 30-minute timeout per run mirror what the
old systemd unit did. A full backfill (`Window.everything()`, what `--full`
does on the CLI) is a checkbox on "Trigger DAG w/ config" in the UI, not a
flag you have to know to type over SSH.

`catchup` is off, so a host that was down runs each DAG once when it comes
back, over the source's lookback. For a source that declares `catch_up_on`
(the daily price sources), that run starts at the last date stored instead
when it is older: a two-week outage is fetched whole, not left as a hole.

## Order book capture

`ingest capture` records Bitstamp's public order-by-order feed: every order
created, changed and deleted, with its id. No account, no key. It is the one
long-running service of this repository (the `capture` service of
`docker-compose.yml`), and it writes files, not rows: the consumer,
`quant-microstructure`, replays a whole day in order, which a file does
without a query (its ADR-015).

**Two pairs** are recorded, `CAPTURE_PAIRS` in `.env`: BTC/USD and NEAR/USD,
chosen on what the feed showed on 12 October 2026 (twenty minutes of BTC/USD,
seven of NEAR/USD), for being the two ends of what Bitstamp offers:

| | BTC/USD | NEAR/USD |
|---|---|---|
| tick | 0.01 on a price of 83,400 | 0.00001 on a price of 5.84 |
| spread, median | 1 tick | 427 ticks |
| time spent at a spread of one tick | 98.5 % | never |
| orders resting at each best price | 5.6 | 1.0 |
| events a second | 135 | 14 |
| trades a minute | 26 | 0.1 |

On BTC/USD the spread is pinned to the tick and orders queue at the best
price: the place in the queue is what matters. On NEAR/USD the tick is no
constraint, orders compete on price and each stands alone at its level; it
trades little, its book is what there is to study. The other large pairs
(ETH, SOL, XRP, DOGE) are of the first kind, at one tick 70 to 90 % of the
time.

XRP/USD was tried and put aside: in twenty minutes its recorded book differed
once from the exchange's (one order, by its quantity), and messages about
orders the recorder did not know were counted, which did not happen on the
five other pairs tried. Something in its messages is not read correctly yet.

```
data/capture/bitstamp/btcusd/btcusd-2026-10-12.qmev            the events, while the day is open
data/capture/bitstamp/btcusd/btcusd-2026-10-11.qmev.xz         the same, once the day is over
data/capture/bitstamp/btcusd/btcusd-2026-10-12.json            the sidecar
data/capture/bitstamp/btcusd/btcusd-2026-10-12.checkpoints.gz  the exchange's snapshots
```

**The events** are in the format of `quant-microstructure`
(`feed/event_file.hpp`): an 8-byte header, then 50-byte little-endian records
— exchange time, receive time, sequence number, order id, price, quantity,
type (Add, Cancel, Trade, Clear) and side. Prices are whole ticks, quantities
whole lots, times nanoseconds since the Unix epoch. One file per pair and per
UTC day; each starts with a Clear and the whole book, so a day replays on its
own, and numbers its events from 1.

**The sidecar** says what version 1 of the format cannot: that the stream is
order by order, the tick and the lot, and how the day went (events, time
range, holes, reconnections, defects, latency).

**How a Bitstamp message becomes an event** was read off the feed and is
written at the top of `capture/bitstamp.py`. The one thing that is not
obvious: an order that trades on arrival is announced as created, then deleted
at the price it traded at. It never rested, so it gives no event of its own;
the Trade is the fill of the resting order it hit, signed by the aggressor.

**Quality.** Three checks run while recording, and each failure leaves a hole
in the numbering of the file (one number skipped), followed by a Clear and a
fresh book, as `quant-microstructure` expects:

- every message names the one before it: a mismatch is a lost message;
- every ten minutes the book is compared with the one the exchange publishes,
  and that snapshot is kept as a checkpoint;
- the connection dropping, or going silent for 30 seconds.

A crossed book and messages about orders the recorder never saw are counted
in the sidecar. Both clocks are kept on every event; Bitstamp's is cut to the
millisecond.

```bash
ingest capture --pair btcusd,nearusd               # record until stopped
ingest capture-verify data/capture/bitstamp/btcusd/btcusd-2026-10-11.qmev.xz
```

`capture-verify` replays a day, compressed or not, the way
`quant-microstructure` will, with a book that refuses an invalid event, and
compares it at each checkpoint with what the exchange published: every order,
its side, price and quantity. Since the exchange's times are cut to the
millisecond, the orders touched in the millisecond of a snapshot are left out
of that comparison.

**Compression.** The open day is a plain file: it is appended to and can be
read at any offset. Once the day is over the service compresses it with xz,
which makes it about four times smaller; `xz -d` gives back the file
`quant-microstructure` reads, byte for byte. xz rather than zstd or gzip: it
is the smallest of the three on these files (4.3 times against 3.8 and 3.1 on
twenty minutes of BTC/USD) and Python reads it without a dependency.

**Retention** is a budget, not a number of days: `CAPTURE_MAX_GB` in `.env`
(40 by default; 0 keeps everything). When the directory weighs more, the
service removes the events and the checkpoints of the oldest days, whatever
the pair, until it fits. The day being recorded is never touched. The sidecar
of a removed day stays, and so do its rows in Postgres: what was computed
from a file outlives it. A day that is gone cannot be had again, Bitstamp
does not serve the history of its orders.

**Volume**, extrapolated from those minutes of a Monday morning: 11.7 million
events and 0.58 GB a day for BTC/USD while the day is open, 0.13 GB once
compressed, plus 16 MB of checkpoints; a tenth of that for NEAR/USD. About
0.17 GB a day in all, so the default budget holds some 240 days. A full day
has not been measured yet, and a busy one will weigh several times more: the
catalogue below measures every day from now on (`events`, `bytes`), and the
budget is to be set again on a measured week.

**Holes.** A message is sometimes missing from the feed: one of BTC/USD's was
lost twice in ten minutes on one connection while another connection, open at
the same time, received it. Each costs a fresh book in the file (0.5 MB for
BTC/USD). The rate is in the catalogue (`gaps`, `chain_breaks`), per day.

**In Postgres**, computed from the files and never the reverse. The capture
itself does not touch the database, so it records through an outage; two
scheduled sources read its directory:

- `capture-catalogue`, every 15 minutes, keeps one row per pair and per day in
  `microstructure.capture_files`: where the events are (NULL once removed),
  their size, the time range, the events, the holes, the checks.
- `microstructure-bars`, every hour, replays the day being recorded and the
  one before it into `microstructure.bars_1m`: per minute, the trades (open,
  high, low, close, volume split by the side of the aggressor, VWAP), the top
  of the book at the end of the minute, the spread averaged over the time the
  book was known, the orders added and cancelled, and the holes of the feed.

`bars_1m` is a plain table, not a TimescaleDB hypertable. A pair is 1,440 rows
a day, half a million a year: the primary key `(venue, pair, ts)` serves that
without help, and the extension would mean another Postgres image for the
whole store. To be looked at again if the aggregates go below the second.

`CAPTURE_DIR` in `.env` is where the files go on the host (default
`./data/capture`, git-ignored); `quant-microstructure` reads them there.

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

**Updating is automatic**: merging into `main` deploys. Every two minutes a
systemd user timer runs `scripts/auto-deploy.sh`, which does nothing until
`origin/main` moves, waits for that commit's CI to be green, then
fast-forwards and runs `scripts/deploy.sh`. If the deploy fails (build, Airflow
health, a DAG import error) it goes back to the previous commit, deploys that
again and leaves the unit `failed`; the failed commit is not retried, push a
fix. Each attempt is listed under the repository's *Deployments* on GitHub.

```bash
cp deploy/autodeploy@.service deploy/autodeploy@.timer ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now autodeploy@data-ingest.timer
journalctl --user -u 'autodeploy@*' -f
```

It only acts when this checkout is on a clean `main` (it is also where the
code is written): on a work branch, or with uncommitted changes, it says so in
the journal and waits. The rollback covers the code and the images, not the
data: a table a new source created, or rows it wrote, stay.

By hand — the same thing, from `main`:

```bash
scripts/deploy.sh           # pull, rebuild, restart what changed, verify
scripts/deploy.sh --check   # the same checks, nothing built or restarted
```

A restart is not enough: the Airflow image installs `data_ingest` when it is
built, and the DAG file reads the sources from that installed package, so a
new source only appears after a rebuild. The script refuses a branch other
than `main` or uncommitted changes (this checkout is also where the stack
runs), fixes nothing silently (a root-owned `airflow/logs` is reported with
the command to fix it), waits for the four Airflow services to be healthy,
fails on a DAG import error, and lists the DAGs still paused. It never starts
the `ingest` service, whose default command runs every source — which a bare
`docker compose up -d` does.

It also starts the capture. A deploy that changes the image (any commit that
touches `src/`, `pyproject.toml` or this README) recreates that container: a few seconds
of the feed are lost, the day's files say so (a hole, then a fresh book), and
the recording goes on in the same files.

The Airflow UI is at `http://127.0.0.1:8080` (login: `AIRFLOW_ADMIN_USER` /
`AIRFLOW_ADMIN_PASSWORD` from `.env`). DAGs start paused; unpause the ones you
want to run on their schedule.

Everything binds to `127.0.0.1`; nothing here is reachable off the server.

## Tests

```bash
pip install -e ".[dev]"
pytest -m "not network"    # unit tests plus integration, no upstream API needed
pytest                     # adds the live yfinance, FRED and Bitstamp tests
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
