# Sources

Every dataset, its table, its write mode and its schedule, followed by what a
consumer needs to know about each one.

| Source | Table | Write mode | Schedule (cron, UTC) |
|---|---|---|---|
| `sp500-prices` | `prices.sp500_daily` | upsert | `0 22 * * 1-5` |
| `commodity-prices` | `prices.commodity_daily` | upsert | `30 23 * * 1-5` |
| `fred-macro` | `macro.fred_series` | versioned | `0 23 * * 1-5` |
| `fx-rates` | `fx.daily_rates` | upsert | `0 23 * * 1-5` |
| `options-chain-snapshot` | `options.chain_snapshot` | snapshot | `30 22 * * 1-5` |
| `options-chain-iv` | `options.chain_iv` | snapshot | after its inputs (see below) |
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
a small fixed list of liquid, options-heavy names (broad ETFs and mega-caps)
plus the European index options on the S&P 500, `^SPX` and `^XSP`, not the
full `sp500-prices` universe — see the source file for why — and is
overridable with `OPTIONS_CHAIN_TICKERS`.

The table is the **raw** chain. Its `implied_volatility` is Yahoo's, backed
out at r = q = 0: no consumer should read it as a vol. Each quote also keeps
its contract:

- `root`, parsed from the contract symbol: the ticker for an equity, and for
  the index options `SPX` (AM-settled monthlies), `SPXW` (PM-settled) or
  `XSP`. SPX and SPXW share expiry dates with settlement times a session
  apart, so the root is part of the primary key, `(date, ticker, expiry,
  root, option_type, strike)`. Chains stored before the contract was
  kept (WP 31 L1) carry `root = ticker`, which is what their contracts' roots were.
- `contract_symbol`, the OCC symbol, and `last_trade_date`, when the contract
  last traded: a bid/ask that has not moved since an old last trade is a stale
  quote. Both are NULL on those earlier chains.

The strike coverage yfinance gives on SPX, XSP and SPY is measured in
[option-coverage.md](option-coverage.md).

Each stored (ticker, date) is **exactly one fetch**: a run's rows for a ticker
replace everything stored for that ticker on that day, in one transaction, so
a same-day re-run never mixes its quotes with an earlier run's. The cost is
accepted on purpose: when Yahoo fails on some expiries, the stored chain lacks
them (the run logs `snapshot is partial`) rather than filling the gap with
older quotes. A ticker the run returns nothing for keeps its earlier snapshot.

Next to the chain, `options.chain_snapshot_loads` is its load registry, so a
consumer can tell whether a snapshot changed by reading one indexed row
instead of the whole chain:

| Column | Type | |
|---|---|---|
| `ticker` | TEXT | primary key |
| `date` | DATE | primary key, the snapshot's `date` |
| `loaded_at` | TIMESTAMPTZ | time of the transaction that last wrote this (ticker, date) |
| `row_count` | INTEGER | rows of this (ticker, date) in `options.chain_snapshot` |
| `checksum` | TEXT | md5 of those rows, see below |

Guarantees to consumers:

- **One row per stored (ticker, date)**: every snapshot in
  `options.chain_snapshot` has its registry row, including those stored
  before the registry existed (filled when it was created).
- **Same transaction as the chain**: a write that touches a (ticker, date)
  updates its registry row before committing, so a reader never sees a new
  registry row with old quotes or the reverse. A failed write rolls back both.
- **Computed from the stored rows** by Postgres, not from what the run
  fetched: `row_count` and `checksum` describe exactly what a reader gets.
- `loaded_at` moves on every write of the (ticker, date), even an identical
  one; `checksum` changes only when the stored rows do. Compare checksums to
  detect a change.

The checksum is reproducible from the table, with `extra_float_digits = 1`
(the Postgres default), `DateStyle = 'ISO, MDY'` and `TimeZone = 'UTC'`. It
changed definition when the contract columns were added: the run that added
them recomputed every registry row, so a checksum compared across that date
differs without the quotes having changed.

```sql
SET TimeZone = 'UTC';
SELECT md5(string_agg(
         ROW(expiry, root, option_type, strike, contract_symbol, last_trade_date,
             bid, ask, last_price, volume, open_interest, implied_volatility)::text,
         '|' ORDER BY expiry, root COLLATE "C", option_type COLLATE "C", strike))
FROM options.chain_snapshot
WHERE ticker = 'SPY' AND date = '2026-11-18';
```

### `options-chain-iv`

The implied vols, forwards and discounts of the stored chains, computed at
ingestion from `options.chain_snapshot` (quant-modeling WP 31). The raw chain
stays as yfinance returned it; a method change is a recomputation.

**When it runs.** It reads stored tables and fetches nothing. Its Airflow
DAG has no cron. It runs once the four DAGs it reads from have succeeded
since its last run:
- `options-chain-snapshot`;
- `sp500-prices` (the American spots);
- `dividend-yields`;
- `equity-dividends`.

FRED is read as of the day's latest stored value and recorded, without
being waited for: SOFR for a day is published the next morning.

**Which days it computes.**
- A routine run looks back five days, catching up from the last computed
  day after a gap. It recomputes the days whose snapshot changed since they
  were computed (`chain_snapshot_loads.loaded_at`) or that were computed by
  another `METHOD_VERSION`.
- `ingest run options-chain-iv --full` recomputes every stored day, whatever
  its state. It is the deliberate command after a method change. Run it from
  the CLI, not from the DAG's `full` switch, which would exceed the task's
  30 minutes.

**How each day is computed.**
1. **Free parity fit per European (root, expiry):**
   - the chains are `^SPX` (roots `SPX`, AM-settled, and `SPXW`) and `^XSP`;
   - the fit is a robust regression of `C − P = DF F − DF K` over the
     strikes within 10 % of the money that are two-sided on both sides,
     weighted by 1 / (spread_C² + spread_P²), with Huber's loss.
2. **The day's discount curve**, from the free fits whose slope is reliable:
   at least 14 days, a rate standard error under 25 bp, a dispersion under
   half the parity band, and within 50 bp of their neighbours. The zero
   rate is interpolated linearly in T. With fewer than three such nodes,
   the curve falls back to FRED (SOFR, then the Treasury CMT yields `DGS1MO`
   to `DGS30`), and every quote of the day is flagged `rate_fallback`. Every
   day stored before SPX was captured is in that case.
3. **European expiries refitted at the curve's DF**, F alone from their own
   parity. On a short expiry the free slope's error on DF is tens of points
   of rate; the free DF stays in `free_discount`.
4. **American expiries:**
   - DF from the same curve (WP 31, D3);
   - F from carry, `(S − PV(D)) e^{(r − q)T}`, with S the stored close of
     the session;
   - the dividends are discrete, projected from `prices.dividends` (the last
     amount at the median gap of the last five ex-dates), **for every ticker
     with a history, the index ETFs included**. The trailing yield of
     `prices.dividend_yields` is only the fallback.
5. **Per quote:**
   - the OTM side of every strike, and both sides within |ln K/F| < 0.02,
     the ITM one flagged `not_otm`;
   - the bid/ask mid as the price;
   - an American mid is de-Americanized on a 201-step Leisen-Reimer tree.
     `early_exercise_premium` is American − European on the same tree, at
     the σ where Black on `mid − premium` gives σ back (a fixed point);
   - Black-76 is inverted by Jäckel's Let's Be Rational.

**Why the ETFs take discrete dividends.** WP 31 §4.3 gave them a continuous
yield. On SPY 2026-11-25/26, two inputs went wrong with it:
- yfinance's trailing yield (0.79 %) is below what the stored payments give
  (7.58 $ over twelve months, 1.06 %);
- a continuous yield spreads the December ex-date (1.9 $) over the year.

With the SPX curve, the median call/put IV gap within |ln K/F| < 0.02 went
from 0.82 to 0.27 vol points, and under 0.1 point on every expiry to
February 2027.

**Times.** `ttm` is in years, ACT/365F:
- from the close of the last NYSE session on or before `date` (13:00 New
  York on an early-close day);
- to the settlement: 09:30 New York on the expiry date for the `SPX` root,
  16:00 for every other.

SPX and XSP options also trade overnight (Cboe GTH, from 20:15 New York).
The 22:30 UTC snapshot falls before it. A snapshot taken during that session
would not be synchronous with the stored closes.

**Rejections and flags.** Quotes are never dropped silently. An expiry
is rejected (`status`, `rejection_reason` in `chain_iv_expiries`; its quotes
flagged `expiry_rejected`) when:

| Exercise | Reason |
|---|---|
| any | `too_few_pairs`: under 5 two-sided pairs (European) or 3 near the money (American) |
| European | `parity_dispersion`: residual dispersion above the median half-band |
| European | `parity_bounds`: over half the pairs outside the bid/ask parity bounds |
| European | `unknown_root`: a root whose settlement is not known (`SPXO`, seen on yfinance); no quote rows |
| American | `parity_dispersion`: the de-Americanized parity forwards near the money spread wider than their half-band |
| American | `no_spot`: no close for the session; no quote rows |

`flags` is a bit set:

| Bit | Flag | Meaning |
|---|---|---|
| 1 | `no_bid` | bid missing or zero |
| 2 | `wide_spread` | (ask − bid) / mid above 50 % |
| 4 | `below_two_ticks` | mid under two ticks (SPX: 0.05/0.10; XSP, SPY, QQQ, IWM: 0.01; other equities: 0.01/0.05) |
| 8 | `stale` | same bid and ask as the previous snapshot, no trade since its session closed |
| 16 | `parity_outlier` | its strike's pair breaks parity: the bid/ask bounds (American: `S e^{−qT} − PV(D) − K ≤ C − P ≤ S − K DF`) or a residual beyond 4 robust σ |
| 32 | `expiry_rejected` | see above |
| 64 | `iv_failed` | no implied vol: no mid, or a price outside Black's range |
| 128 | `not_otm` | the ITM side, kept near the forward for the call/put diagnostic only |
| 256 | `short_maturity` | under 7 days |
| 512 | `rate_fallback` | DF from FRED, not from the day's European parity |

A bit never changes meaning; new ones are added. The thresholds are starting
values, to be fixed by WP 31's literature review (lot L0).

**Tables.**
- `options.chain_iv`: one row per kept quote and day, keyed like the chain:
  `(date, ticker, expiry, root, option_type, strike)`. The columns are those
  of WP 31 §4.2.
- `options.chain_iv_expiries`: one row per (date, ticker, expiry, root), with
  the following columns:
  - F and DF, and their sources;
  - `implied_rate` (−ln DF / T) and `implied_yield` (r − ln(F/S) / T, S the
    stored close, so contaminated by any asynchrony);
  - the free parity DF and its standard error (European);
  - the parity forward near the money and its gap from the carry forward
    (American);
  - the dispersion and half-band;
  - the pair and bound-violation counts;
  - the status and reason.
- `options.chain_iv_loads`: the load registry, one row per (ticker, date),
  written in the same transaction as the quotes. It holds:
  - `row_count` and `checksum`, the md5 of the rows as text, ordered like
    the chain's, with the settings of `chain_snapshot_loads`;
  - `method_version`;
  - `valuation_time`;
  - the curve's source and date;
  - the spot and dividend dates.
- A ticker whose recomputation keeps no quote loses its earlier rows, so the
  three tables always describe one computation.

**Measured on 2026-11-25/26:**
- SPX's one-year ATM vol is 19.6 %; SPY's, de-Americanized, 20.0–20.2 %.
- The SPX implied rate runs from 4.4 % to 5.1 %.
- A full day of the ten American tickers takes about a minute; the three
  of 2026-11-26 (SPX, XSP, SPY), 20 s.

`dividend-yields` tracks the same universe less the index options (imported
from `options_chain.py`, not a second list; `^SPX` and `^XSP` take their
forward from their own parity) because it exists to serve the same
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
