"""Implied vols, forwards and discounts of the stored option chains.

Computed at ingestion from `options.chain_snapshot`, which stays untouched,
into `options.chain_iv`: one row per kept quote and per day (quant-modeling
WP 31, §4). Yahoo's own `implied_volatility` is backed out at r = q = 0 and
is not used. An implied vol here is the sigma Black-76 maps to the quote's
European price against the expiry's forward F and discount DF: a quoting
convention, which needs F and DF and no model of the dynamics.

Per day, in this order:

1. **The European chains** (^SPX's SPX and SPXW roots, ^XSP): each
   (root, expiry) gets a free parity fit, C - P = DF F - DF K, robust and
   weighted by the spreads (options/parity.py).
2. **The day's discount curve**, from the free fits whose slope is reliable
   (options/rates.py). When there are too few, as on every day stored before
   SPX was captured, the curve falls back to FRED (SOFR, then the Treasury
   constant-maturity yields), and every quote of the day is flagged
   `rate_fallback`.
3. **Every European expiry is refitted with DF from the curve**, so F alone
   comes from its own parity: on a short expiry, the free slope's error on DF
   is tens of points of rate. The free DF stays in `chain_iv_expiries`.
4. **The American chains** (ETFs and stocks): DF from the same curve; F from
   carry, `(S - PV(dividends)) exp((r - q) T)`, with the discrete cash
   dividends projected from prices.dividends (the last amount, at the last
   observed frequency) for every ticker that has a history, index ETFs
   included, and the continuous trailing yield of prices.dividend_yields as
   the fallback (see `carry_of`). Near the money, the parity forward K + (C - P) / DF is kept as
   a diagnostic: its gap from the carry forward measures the spot's
   asynchrony (SPY's options close at 16:15, its stored close is 16:00's), and
   its dispersion rejects an expiry with inconsistent quotes (SPY's
   end-of-quarter expiries).
5. **Per quote**: one side per strike, the OTM one (puts below F, calls at or
   above it), both within |ln K/F| < 0.02 for the call/put diagnostic, the
   side not used flagged `not_otm`. The ITM side outside that band is not
   stored here (the raw chain keeps it). The price is the bid/ask mid; an
   American mid is de-Americanized on a Leisen-Reimer tree
   (options/american.py); Black-76 is inverted by Jäckel's Let's Be Rational.

Flags, never silent drops: a quote that fails a check is stored with a bit
set in `flags` (see `Flag`) and its consumer decides. An expiry whose forward
cannot be computed at all has no quote rows; `chain_iv_expiries` says why.

Times: `ttm` is in years, ACT/365F, from the close of the NYSE session the
chain's quotes are from (the snapshot runs after the close; a holiday's
snapshot holds the previous session's quotes) to the settlement: 09:30 New
York on the expiry date for the AM-settled SPX root, 16:00 for every other.

Runs and backfill: a routine run recomputes the days of its window whose
snapshot changed since they were computed, or that were computed by another
`METHOD_VERSION`. `ingest run options-chain-iv --full` recomputes every
stored day, whatever its state: the deliberate command after a method change.

Thresholds (spread, ticks, maturity, parity dispersion) are starting values,
to be fixed by the literature review of WP 31 (lot L0, §6-§7).
"""

from __future__ import annotations

import enum
import logging
from dataclasses import dataclass, field
from datetime import date, datetime, time as clock, timedelta
from typing import Dict, Iterator, List, Optional, Tuple
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import pandas_market_calendars as market_calendars
import psycopg
from psycopg import sql

from ..core.source import Source, Window
from ..core.spec import Column, TableSpec, WriteMode
from ..options import american, black, parity, rates
from .options_chain import REGISTRY_TABLE as SNAPSHOT_REGISTRY

logger = logging.getLogger("data_ingest.options_chain_iv")

#: Version of the method. Bump it when a change alters any stored number:
#: routine runs then recompute the days they touch, and `--full` the rest.
METHOD_VERSION = 1

#: Tickers whose options are European, with the close standing for their
#: underlying in prices.sp500_daily and its scale.
EUROPEAN_UNDERLYING = {"^SPX": ("^GSPC", 1.0), "^XSP": ("^GSPC", 0.1)}
#: Settlement of each root: AM (09:30 New York on the expiry date) or PM
#: (16:00). An equity's root is its ticker, PM-settled. A European root not
#: listed here (SPXO, seen on yfinance) is rejected until its settlement is
#: known.
SETTLEMENT = {"SPX": "am", "SPXW": "pm", "XSP": "pm"}

#: Both sides are kept within this |ln K/F|, for the call/put diagnostic.
BOTH_SIDES_BAND = 0.02
#: Relative spread above which a quote is flagged `wide_spread` (§6: 50 %).
WIDE_SPREAD = 0.50
#: Maturity under which a quote is flagged `short_maturity` (§6: 7 days).
SHORT_MATURITY = 7.0 / 365.0
#: A European expiry whose parity residuals spread wider than this many
#: times its median parity half-band is rejected...
MAX_PARITY_DISPERSION = 1.0
#: ...and so is one where more than this share of pairs break the bounds.
MAX_BOUND_VIOLATIONS = 0.5
#: American diagnostics: the band around the carry forward where the parity
#: forward is measured, and the fewest pairs it needs.
AMERICAN_PARITY_BAND = 0.05
AMERICAN_MIN_PAIRS = 3
#: An American expiry whose parity forwards spread wider than this many
#: times their median half-band is rejected (SPY's end-of-quarter expiries:
#: 3.5-5 $ of dispersion against 0.4 $ on the monthlies).
AMERICAN_MAX_DISPERSION = 1.0
#: Dividends: the most ex-dates the frequency is measured on, and how many
#: gaps without one end a dividend history.
DIVIDEND_HISTORY = 5
DIVIDEND_STOPPED_AFTER = 2.0

NEW_YORK = ZoneInfo("America/New_York")
_NYSE = market_calendars.get_calendar("NYSE")
_SECONDS_PER_YEAR = 365.0 * 86400.0


class Flag(enum.IntFlag):
    """Bits of `options.chain_iv.flags`. Part of the contract in
    docs/sources.md: a bit's meaning never changes, new ones are added."""

    NO_BID = 1             # bid missing or zero
    WIDE_SPREAD = 2        # (ask - bid) / mid above WIDE_SPREAD
    BELOW_TWO_TICKS = 4    # mid under two ticks of its product
    STALE = 8              # same bid and ask as the previous snapshot, no trade since
    PARITY_OUTLIER = 16    # its strike's pair breaks parity (bounds, or a far residual)
    EXPIRY_REJECTED = 32   # its expiry failed the parity checks (chain_iv_expiries says why)
    IV_FAILED = 64         # no implied vol: price outside Black's range, or no mid
    NOT_OTM = 128          # the ITM side, kept within BOTH_SIDES_BAND for the diagnostic only
    SHORT_MATURITY = 256   # under SHORT_MATURITY
    RATE_FALLBACK = 512    # DF from the FRED fallback, not from the day's European parity


EXPIRIES_TABLE = "chain_iv_expiries"
LOADS_TABLE = "chain_iv_loads"

EXPIRY_COLUMNS = (
    Column("date", "DATE", nullable=False),
    Column("ticker", "TEXT", nullable=False),
    Column("expiry", "DATE", nullable=False),
    Column("root", "TEXT", nullable=False),
    Column("exercise", "TEXT", nullable=False),
    Column("ttm", "DOUBLE PRECISION"),
    Column("forward", "DOUBLE PRECISION"),
    Column("discount", "DOUBLE PRECISION"),
    Column("forward_source", "TEXT"),  # parity | carry
    Column("discount_source", "TEXT"),  # parity | fred
    Column("implied_rate", "DOUBLE PRECISION"),  # -ln(DF) / T
    Column("implied_yield", "DOUBLE PRECISION"),  # r - ln(F / S) / T, S the stored close
    Column("spot", "DOUBLE PRECISION"),
    Column("free_discount", "DOUBLE PRECISION"),  # European: the expiry's own parity DF
    Column("free_discount_se", "DOUBLE PRECISION"),
    Column("parity_forward", "DOUBLE PRECISION"),  # American: median K + (C - P) / DF near the money
    Column("carry_forward_gap", "DOUBLE PRECISION"),  # American: parity_forward - forward
    Column("parity_dispersion", "DOUBLE PRECISION"),
    Column("parity_half_band", "DOUBLE PRECISION"),
    Column("n_pairs", "INTEGER"),
    Column("n_bound_violations", "INTEGER"),
    Column("n_quotes", "INTEGER"),
    Column("status", "TEXT", nullable=False),  # accepted | rejected
    Column("rejection_reason", "TEXT"),
    Column("method_version", "INTEGER", nullable=False),
)
EXPIRY_KEY = ("date", "ticker", "expiry", "root")

LOAD_COLUMNS = (
    Column("ticker", "TEXT", nullable=False),
    Column("date", "DATE", nullable=False),
    Column("loaded_at", "TIMESTAMPTZ", nullable=False),
    Column("row_count", "INTEGER", nullable=False),
    Column("checksum", "TEXT"),
    Column("method_version", "INTEGER", nullable=False),
    Column("valuation_time", "TIMESTAMPTZ"),
    Column("curve_source", "TEXT"),
    Column("curve_date", "DATE"),
    Column("spot_date", "DATE"),
    Column("dividend_date", "DATE"),
)

#: The columns of a chain_iv row the registry's checksum covers, in order.
_CHECKSUM_COLUMNS = (
    "expiry", "root", "option_type", "strike", "ttm", "mid", "bid", "ask", "rel_spread",
    "exercise", "forward", "discount", "forward_source", "price_european",
    "early_exercise_premium", "implied_vol", "log_moneyness", "vega", "flags", "method_version",
)


@dataclass
class _Day:
    """What one day's computation hands to `after_write`."""

    day: date
    expiries: pd.DataFrame
    loads: List[dict] = field(default_factory=list)


class OptionsChainIV(Source):
    name = "options-chain-iv"
    description = "Implied vols, forwards and discounts of the stored option chains (computed)"
    write_mode = WriteMode.SNAPSHOT
    inputs = ("options-chain-snapshot", "sp500-prices", "dividend-yields", "equity-dividends")
    lookback_days = 5
    catch_up_on = "date"

    table = TableSpec(
        schema="options",
        name="chain_iv",
        columns=(
            Column("date", "DATE", nullable=False),
            Column("ticker", "TEXT", nullable=False),
            Column("expiry", "DATE", nullable=False),
            Column("root", "TEXT", nullable=False),
            Column("option_type", "TEXT", nullable=False),
            Column("strike", "DOUBLE PRECISION", nullable=False),
            Column("ttm", "DOUBLE PRECISION", nullable=False),
            Column("mid", "DOUBLE PRECISION"),
            Column("bid", "DOUBLE PRECISION"),
            Column("ask", "DOUBLE PRECISION"),
            Column("rel_spread", "DOUBLE PRECISION"),
            Column("exercise", "TEXT", nullable=False),  # european | american
            Column("forward", "DOUBLE PRECISION", nullable=False),
            Column("discount", "DOUBLE PRECISION", nullable=False),
            Column("forward_source", "TEXT", nullable=False),  # parity | carry
            Column("price_european", "DOUBLE PRECISION"),
            Column("early_exercise_premium", "DOUBLE PRECISION"),
            Column("implied_vol", "DOUBLE PRECISION"),
            Column("log_moneyness", "DOUBLE PRECISION", nullable=False),
            Column("vega", "DOUBLE PRECISION"),
            Column("flags", "INTEGER", nullable=False),
            Column("method_version", "INTEGER", nullable=False),
        ),
        primary_key=("date", "ticker", "expiry", "root", "option_type", "strike"),
        indexes=(("ticker", "date"),),
        snapshot_key=("ticker", "date"),
    )

    def __init__(self) -> None:
        self._pending: Optional[_Day] = None

    def fetch(self, window: Window) -> pd.DataFrame:
        raise TypeError(f"{self.name} is computed from stored tables: run it through frames(db, window)")

    # ------------------------------------------------------------ tables

    def _qualified(self, name: str) -> sql.Composed:
        return sql.SQL("{}.{}").format(sql.Identifier(self.table.schema), sql.Identifier(name))

    def ensure_companions(self, conn: psycopg.Connection) -> None:
        for name, columns, key in (
            (EXPIRIES_TABLE, EXPIRY_COLUMNS, EXPIRY_KEY),
            (LOADS_TABLE, LOAD_COLUMNS, ("ticker", "date")),
        ):
            body = ",\n    ".join([c.ddl() for c in columns] + [f"PRIMARY KEY ({', '.join(key)})"])
            conn.execute(sql.SQL("CREATE TABLE IF NOT EXISTS {} (\n    " + body + "\n)").format(self._qualified(name)))

    def after_write(self, cur: psycopg.Cursor, frame: pd.DataFrame) -> None:
        """Writes the day's expiries and registry rows with its quotes. A
        ticker the day computed no quote for loses the rows an earlier run
        stored for it, so the three tables always describe one computation."""
        pending = self._pending
        if pending is None:
            return
        tickers = [load["ticker"] for load in pending.loads]
        written = set(frame["ticker"].unique())
        empty = [t for t in tickers if t not in written]
        if empty:
            cur.execute(
                sql.SQL("DELETE FROM {} WHERE date = %s AND ticker = ANY(%s)").format(self._qualified(self.table.name)),
                (pending.day, empty),
            )
        cur.execute(
            sql.SQL("DELETE FROM {} WHERE date = %s AND ticker = ANY(%s)").format(self._qualified(EXPIRIES_TABLE)),
            (pending.day, tickers),
        )
        if not pending.expiries.empty:
            names = [c.name for c in EXPIRY_COLUMNS]
            rows = pending.expiries[names].astype(object)
            rows = rows.where(pending.expiries[names].notna(), None)
            with cur.copy(
                sql.SQL("COPY {} ({}) FROM STDIN").format(
                    self._qualified(EXPIRIES_TABLE), sql.SQL(", ").join(sql.Identifier(n) for n in names)
                )
            ) as copy:
                for row in rows.itertuples(index=False, name=None):
                    copy.write_row(row)
        self._record_loads(cur, pending)

    def _record_loads(self, cur: psycopg.Cursor, pending: _Day) -> None:
        cur.execute("SET LOCAL extra_float_digits = 1")
        cur.execute("SET LOCAL DateStyle = 'ISO, MDY'")
        cur.execute("SET LOCAL TimeZone = 'UTC'")
        row = sql.SQL("ROW({})::text").format(
            sql.SQL(", ").join(sql.SQL("c.{}").format(sql.Identifier(n)) for n in _CHECKSUM_COLUMNS)
        )
        for load in pending.loads:
            cur.execute(
                sql.SQL(
                    "INSERT INTO {loads} (ticker, date, loaded_at, row_count, checksum, method_version, "
                    "valuation_time, curve_source, curve_date, spot_date, dividend_date) "
                    "SELECT %(ticker)s, %(date)s, now(), count(c.*), "
                    "md5(string_agg({row}, '|' ORDER BY c.expiry, c.root COLLATE \"C\", "
                    "c.option_type COLLATE \"C\", c.strike)), "
                    "%(method_version)s, %(valuation_time)s, %(curve_source)s, %(curve_date)s, "
                    "%(spot_date)s, %(dividend_date)s "
                    "FROM {chain} c WHERE c.ticker = %(ticker)s AND c.date = %(date)s "
                    "ON CONFLICT (ticker, date) DO UPDATE SET loaded_at = EXCLUDED.loaded_at, "
                    "row_count = EXCLUDED.row_count, checksum = EXCLUDED.checksum, "
                    "method_version = EXCLUDED.method_version, valuation_time = EXCLUDED.valuation_time, "
                    "curve_source = EXCLUDED.curve_source, curve_date = EXCLUDED.curve_date, "
                    "spot_date = EXCLUDED.spot_date, dividend_date = EXCLUDED.dividend_date"
                ).format(loads=self._qualified(LOADS_TABLE), row=row, chain=self._qualified(self.table.name)),
                load,
            )

    # -------------------------------------------------------------- days

    def days_to_compute(self, conn: psycopg.Connection, window: Window) -> List[date]:
        """The stored snapshot days of the window to (re)compute: all of
        them with `window.full`, otherwise those not yet computed, computed
        before their snapshot last changed, or by another METHOD_VERSION."""
        if not _table_exists(conn, f"{self.table.schema}.{SNAPSHOT_REGISTRY}"):
            return []
        conditions, params = [], {"version": METHOD_VERSION}
        if not window.full:
            conditions.append(sql.SQL("s.date >= %(start)s AND s.date <= %(end)s"))
            params.update(start=window.start or date.min, end=window.end or date.max)
        computed = _table_exists(conn, f"{self.table.schema}.{LOADS_TABLE}")
        if computed and not window.full:
            conditions.append(sql.SQL(
                "(i.ticker IS NULL OR i.loaded_at < s.loaded_at OR i.method_version <> %(version)s)"
            ))
        join = sql.SQL("LEFT JOIN {} i ON i.ticker = s.ticker AND i.date = s.date").format(
            self._qualified(LOADS_TABLE)
        ) if computed else sql.SQL("")
        where = sql.SQL("WHERE ") + sql.SQL(" AND ").join(conditions) if conditions else sql.SQL("")
        query = sql.SQL("SELECT DISTINCT s.date FROM {} s {} {} ORDER BY 1").format(
            self._qualified(SNAPSHOT_REGISTRY), join, where
        )
        return [r[0] for r in conn.execute(query, params).fetchall()]

    def frames(self, db, window: Window) -> Iterator[pd.DataFrame]:
        with db.connect() as conn:
            days = self.days_to_compute(conn, window)
        logger.info("%s: %d day(s) to compute", self.name, len(days))
        for day in days:
            with db.connect() as conn:
                quotes, pending = compute_day(conn, day, self.table.schema)
            self._pending = pending
            accepted = int((pending.expiries["status"] == "accepted").sum()) if not pending.expiries.empty else 0
            logger.info(
                "%s %s: %d quotes, %d expiries (%d accepted), curve %s",
                self.name, day, len(quotes), len(pending.expiries), accepted,
                pending.loads[0]["curve_source"] if pending.loads else None,
            )
            yield quotes


# ------------------------------------------------------------------ times


def valuation_time(day: date) -> pd.Timestamp:
    """The close of the last NYSE session on or before `day`, in UTC."""
    schedule = _NYSE.schedule(start_date=day - timedelta(days=14), end_date=day)
    if schedule.empty:
        raise ValueError(f"no NYSE session in the two weeks before {day}")
    return pd.Timestamp(schedule["market_close"].iloc[-1]).tz_convert("UTC")


def settlement_time(expiry: date, settlement: str) -> pd.Timestamp:
    at = clock(9, 30) if settlement == "am" else clock(16, 0)
    return pd.Timestamp(datetime.combine(expiry, at, tzinfo=NEW_YORK)).tz_convert("UTC")


def years_between(start: pd.Timestamp, end: pd.Timestamp) -> float:
    return (end - start).total_seconds() / _SECONDS_PER_YEAR


# ----------------------------------------------------------------- quotes


def tick_size(root: str, price: np.ndarray) -> np.ndarray:
    """The minimum tick of a premium: SPX and SPXW trade in 0.05 under 3 $
    and 0.10 above; XSP and the penny-pilot ETFs in 0.01 at any price;
    other equity options in 0.01 under 3 $ and 0.05 above. A simplification
    of the exchanges' tables, enough for a two-tick floor."""
    price = np.asarray(price, dtype=float)
    if root.startswith("SPX"):
        return np.where(price < 3.0, 0.05, 0.10)
    if root in {"XSP", "SPY", "QQQ", "IWM"}:
        return np.full(price.shape, 0.01)
    return np.where(price < 3.0, 0.01, 0.05)


def _mid_and_spread(quotes: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray]:
    bid = quotes["bid"].to_numpy(dtype=float)
    ask = quotes["ask"].to_numpy(dtype=float)
    valid = np.isfinite(ask) & (ask > 0) & np.isfinite(bid) & (bid >= 0) & (ask >= bid)
    mid = np.where(valid, (np.nan_to_num(bid) + ask) / 2.0, np.nan)
    rel = np.where(valid & (mid > 0), (ask - np.nan_to_num(bid)) / np.where(mid > 0, mid, 1.0), np.nan)
    return mid, rel


@dataclass(frozen=True)
class _Expiry:
    """One (ticker, root, expiry)'s forward and what flags its quotes."""

    ttm: float
    forward: float
    discount: float
    exercise: str
    forward_source: str
    base_flags: int
    outlier_strikes: np.ndarray
    carry: Optional[american.Carry] = None


def quote_rows(group: pd.DataFrame, exp: _Expiry, stale: np.ndarray, band: float = BOTH_SIDES_BAND) -> pd.DataFrame:
    """The quotes of one expiry, with their European price, implied vol and
    flags: the OTM side of every strike, and both sides within `band` of the
    forward (the ITM one flagged NOT_OTM). `group` holds the raw quotes;
    `stale` is aligned to it. The stored rows take `band` = BOTH_SIDES_BAND;
    a wider one serves a diagnostic (see `_american_diagnostics`)."""
    group = group.reset_index(drop=True)
    strike = group["strike"].to_numpy(dtype=float)
    is_call = (group["option_type"] == "call").to_numpy()
    log_moneyness = np.log(strike / exp.forward)
    otm = np.where(strike < exp.forward, ~is_call, is_call)
    keep = otm | (np.abs(log_moneyness) < band)
    group, strike, is_call, log_moneyness, otm, stale = (
        group[keep].reset_index(drop=True), strike[keep], is_call[keep], log_moneyness[keep], otm[keep], stale[keep]
    )
    if group.empty:
        return pd.DataFrame()

    mid, rel_spread = _mid_and_spread(group)
    bid = group["bid"].to_numpy(dtype=float)
    root = group["root"].iloc[0]

    premium = np.zeros_like(mid)
    price_european = mid.copy()
    if exp.exercise == "american":
        priced = np.isfinite(mid) & (mid > 0)
        if priced.any():
            result = american.deamericanize(mid[priced], strike[priced], is_call[priced], exp.carry, exp.ttm)
            premium[priced] = result.early_exercise_premium
            price_european[priced] = result.price_european
            # Not converged: the American mid has no σ on the tree.
            price_european[np.flatnonzero(priced)[~result.converged]] = np.nan
        premium[~priced] = np.nan
    vol = black.implied_vol(price_european, exp.forward, strike, exp.ttm, exp.discount, is_call)
    vega = np.where(np.isfinite(vol), black.vega(exp.forward, strike, exp.ttm, np.nan_to_num(vol, nan=1.0), exp.discount), np.nan)

    flags = np.full(len(group), exp.base_flags, dtype=np.int64)
    flags |= np.where(~np.isfinite(bid) | (bid <= 0), int(Flag.NO_BID), 0)
    flags |= np.where(np.nan_to_num(rel_spread, nan=0.0) > WIDE_SPREAD, int(Flag.WIDE_SPREAD), 0)
    flags |= np.where(np.isfinite(mid) & (mid < 2.0 * tick_size(root, mid)), int(Flag.BELOW_TWO_TICKS), 0)
    flags |= np.where(stale, int(Flag.STALE), 0)
    flags |= np.where(np.isin(strike, exp.outlier_strikes), int(Flag.PARITY_OUTLIER), 0)
    flags |= np.where(~np.isfinite(vol), int(Flag.IV_FAILED), 0)
    flags |= np.where(~otm, int(Flag.NOT_OTM), 0)
    if exp.ttm < SHORT_MATURITY:
        flags |= int(Flag.SHORT_MATURITY)

    return pd.DataFrame({
        "date": group["date"],
        "ticker": group["ticker"],
        "expiry": group["expiry"],
        "root": group["root"],
        "option_type": group["option_type"],
        "strike": strike,
        "ttm": exp.ttm,
        "mid": mid,
        "bid": group["bid"].to_numpy(dtype=float),
        "ask": group["ask"].to_numpy(dtype=float),
        "rel_spread": rel_spread,
        "exercise": exp.exercise,
        "forward": exp.forward,
        "discount": exp.discount,
        "forward_source": exp.forward_source,
        "price_european": price_european,
        "early_exercise_premium": premium,
        "implied_vol": vol,
        "log_moneyness": log_moneyness,
        "vega": vega,
        "flags": flags,
        "method_version": METHOD_VERSION,
    })


# ------------------------------------------------------------------ inputs


def _read(conn: psycopg.Connection, query: str, params) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(query, params)
        columns = [d.name for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=columns)


def _chain(conn: psycopg.Connection, schema: str, day: date) -> pd.DataFrame:
    return _read(
        conn,
        sql.SQL(
            "SELECT date, ticker, expiry, root, option_type, strike, bid, ask, last_trade_date "
            "FROM {}.chain_snapshot WHERE date = %s"
        ).format(sql.Identifier(schema)),
        (day,),
    )


def _stale(conn: psycopg.Connection, schema: str, day: date, chain: pd.DataFrame) -> np.ndarray:
    """Per quote of `chain`: the same bid and ask as in the ticker's previous
    snapshot, and no trade since that snapshot's session closed. Unknown, so
    False, where the last trade time is not stored."""
    previous = _read(
        conn,
        sql.SQL(
            "SELECT DISTINCT ON (ticker) ticker, date FROM {}.{} WHERE date < %s ORDER BY ticker, date DESC"
        ).format(sql.Identifier(schema), sql.Identifier(SNAPSHOT_REGISTRY)),
        (day,),
    )
    if previous.empty or chain["last_trade_date"].isna().all():
        return np.zeros(len(chain), dtype=bool)
    before = _read(
        conn,
        sql.SQL(
            "SELECT c.ticker, c.date AS previous_date, c.expiry, c.root, c.option_type, c.strike, "
            "c.bid AS previous_bid, c.ask AS previous_ask FROM {}.chain_snapshot c "
            "JOIN unnest(%s::text[], %s::date[]) AS p(ticker, date) ON c.ticker = p.ticker AND c.date = p.date"
        ).format(sql.Identifier(schema)),
        (list(previous["ticker"]), list(previous["date"])),
    )
    key = ["ticker", "expiry", "root", "option_type", "strike"]
    merged = chain[key + ["bid", "ask", "last_trade_date"]].merge(before, on=key, how="left")
    closes = {d: valuation_time(d) for d in merged["previous_date"].dropna().unique()}
    previous_close = merged["previous_date"].map(closes)
    last_trade = pd.to_datetime(merged["last_trade_date"], utc=True)
    same = (merged["bid"] == merged["previous_bid"]) & (merged["ask"] == merged["previous_ask"])
    return (same & last_trade.notna() & previous_close.notna() & (last_trade < previous_close)).to_numpy()


def _closes(conn: psycopg.Connection, tickers: List[str], session: date) -> Dict[str, float]:
    rows = _read(
        conn, "SELECT ticker, close FROM prices.sp500_daily WHERE ticker = ANY(%s) AND date = %s", (tickers, session)
    ) if _table_exists(conn, "prices.sp500_daily") else pd.DataFrame(columns=["ticker", "close"])
    return {r.ticker: float(r.close) for r in rows.itertuples() if r.close is not None}


def _yields(conn: psycopg.Connection, tickers: List[str], day: date) -> Dict[str, Tuple[date, float]]:
    if not _table_exists(conn, "prices.dividend_yields"):
        return {}
    rows = _read(
        conn,
        "SELECT DISTINCT ON (ticker) ticker, date, trailing_yield FROM prices.dividend_yields "
        "WHERE ticker = ANY(%s) AND date <= %s AND trailing_yield IS NOT NULL ORDER BY ticker, date DESC",
        (tickers, day),
    )
    return {r.ticker: (r.date, float(r.trailing_yield)) for r in rows.itertuples()}


def _dividend_history(conn: psycopg.Connection, tickers: List[str], day: date) -> Dict[str, pd.DataFrame]:
    if not _table_exists(conn, "prices.dividends"):
        return {}
    rows = _read(
        conn,
        "SELECT ticker, ex_date, amount FROM prices.dividends WHERE ticker = ANY(%s) AND ex_date <= %s "
        "ORDER BY ticker, ex_date",
        (tickers, day + timedelta(days=400)),
    )
    return {t: g.reset_index(drop=True) for t, g in rows.groupby("ticker")}


def _fred(conn: psycopg.Connection, day: date) -> Dict[str, Tuple[date, float]]:
    if not _table_exists(conn, "macro.fred_series_latest"):
        return {}
    rows = _read(
        conn,
        "SELECT DISTINCT ON (series_id) series_id, date, value FROM macro.fred_series_latest "
        "WHERE series_id = ANY(%s) AND date <= %s AND value IS NOT NULL ORDER BY series_id, date DESC",
        (list(rates.FRED_NODES), day),
    )
    return {r.series_id: (r.date, float(r.value)) for r in rows.itertuples()}


def _table_exists(conn: psycopg.Connection, qualified: str) -> bool:
    return conn.execute("SELECT to_regclass(%s)", (qualified,)).fetchone()[0] is not None


def project_dividends(history: Optional[pd.DataFrame], valuation: date, horizon: date) -> List[Tuple[date, float]]:
    """The cash dividends expected in (valuation, horizon]: those already
    known, then the last amount repeated at the median gap of the last
    DIVIDEND_HISTORY ex-dates. None when the ticker pays none, or stopped
    (no ex-date for DIVIDEND_STOPPED_AFTER gaps)."""
    if history is None or history.empty:
        return []
    ex_dates = list(pd.to_datetime(history["ex_date"]).dt.date)
    amounts = list(history["amount"].astype(float))
    known = [(d, a) for d, a in zip(ex_dates, amounts) if valuation < d <= horizon]
    if len(ex_dates) < 2:
        return known
    recent = ex_dates[-DIVIDEND_HISTORY:]
    gap = timedelta(days=float(np.median(np.diff([d.toordinal() for d in recent]))))
    if gap.days <= 0:
        return known
    last_date, last_amount = ex_dates[-1], amounts[-1]
    if last_date < valuation and (valuation - last_date) > DIVIDEND_STOPPED_AFTER * gap:
        return known
    projected = list(known)
    next_date = last_date + gap
    while next_date <= horizon:
        if next_date > valuation:
            projected.append((next_date, last_amount))
        next_date += gap
    return projected


# -------------------------------------------------------------------- day


def _expiry_row(day, ticker, expiry, root, exercise, **values) -> dict:
    row = {c.name: None for c in EXPIRY_COLUMNS}
    row.update(date=day, ticker=ticker, expiry=expiry, root=root, exercise=exercise, method_version=METHOD_VERSION)
    row.update(values)
    return row


@dataclass
class DayInputs:
    """Everything one day's computation reads, so that `compute` needs no
    database (and tests can build it by hand)."""

    day: date
    #: The day's raw quotes: date, ticker, expiry, root, option_type,
    #: strike, bid, ask, last_trade_date.
    chain: pd.DataFrame
    #: Per quote of `chain`, aligned to its index: a stale quote.
    stale: np.ndarray
    valuation: pd.Timestamp
    #: Closes of the American tickers and of the European underlyings
    #: (^GSPC), at the valuation session.
    closes: Dict[str, float] = field(default_factory=dict)
    #: Each American ticker's stored cash dividends (ex_date, amount).
    dividends: Dict[str, pd.DataFrame] = field(default_factory=dict)
    #: Each American ticker's latest trailing yield: (date, yield).
    yields: Dict[str, Tuple[date, float]] = field(default_factory=dict)
    #: The FRED fallback's series: (date, percent).
    fred: Dict[str, Tuple[date, float]] = field(default_factory=dict)

    @property
    def session(self) -> date:
        return self.valuation.tz_convert(NEW_YORK).date()


def load_inputs(conn: psycopg.Connection, day: date, schema: str = "options") -> DayInputs:
    chain = _chain(conn, schema, day)
    valuation = valuation_time(day)
    inputs = DayInputs(day=day, chain=chain, stale=np.zeros(len(chain), dtype=bool), valuation=valuation)
    if chain.empty:
        return inputs
    inputs.stale = _stale(conn, schema, day, chain)
    american_tickers = [t for t in sorted(chain["ticker"].unique()) if t not in EUROPEAN_UNDERLYING]
    underlyings = sorted({u for u, _ in EUROPEAN_UNDERLYING.values()})
    inputs.closes = _closes(conn, american_tickers + underlyings, inputs.session)
    inputs.dividends = _dividend_history(conn, american_tickers, day)
    inputs.yields = _yields(conn, american_tickers, day)
    inputs.fred = _fred(conn, day)
    return inputs


def compute_day(conn: psycopg.Connection, day: date, schema: str = "options") -> Tuple[pd.DataFrame, _Day]:
    """Every kept quote of one stored day, and its expiries and registry rows."""
    return compute(load_inputs(conn, day, schema))


def carry_of(ticker: str, inputs: DayInputs, spot: float, rate: float, expiry: date) -> Tuple[american.Carry, Optional[date]]:
    """The carry of an American underlying to one expiry, and the date of
    the dividend data it rests on.

    Discrete cash dividends whenever the ticker has a dividend history, an
    index ETF included: SPY's quarterly ex-dates move its short forwards by
    a whole dividend (1.9 $ on 2026-12-19), which a continuous yield spreads
    out. On SPY 2026-11-25/26 this took the median call/put IV gap within
    |ln K/F| < 0.02 from 0.8 to 0.3 vol points (docs/sources.md). The
    continuous trailing yield (prices.dividend_yields) is the fallback for a
    ticker without history; none, for one without either."""
    history = inputs.dividends.get(ticker)
    if history is not None and not history.empty:
        cash = project_dividends(history, inputs.session, expiry)
        dividends = tuple(((d - inputs.session).days / 365.0, a) for d, a in cash)
        return american.Carry(spot=spot, rate=rate, dividends=dividends), history["ex_date"].iloc[-1]
    if ticker in inputs.yields:
        yield_date, q = inputs.yields[ticker]
        return american.Carry(spot=spot, rate=rate, dividend_yield=q), yield_date
    return american.Carry(spot=spot, rate=rate), None


def compute(inputs: DayInputs) -> Tuple[pd.DataFrame, _Day]:
    """The computation of one day from its inputs (module docstring, steps 1-5)."""
    day, chain, valuation, session, closes = inputs.day, inputs.chain, inputs.valuation, inputs.session, inputs.closes
    if chain.empty:
        return pd.DataFrame(), _Day(day, pd.DataFrame(columns=[c.name for c in EXPIRY_COLUMNS]))
    chain = chain.reset_index(drop=True)
    stale = pd.Series(np.asarray(inputs.stale, dtype=bool), index=chain.index)

    tickers = sorted(chain["ticker"].unique())
    european = [t for t in tickers if t in EUROPEAN_UNDERLYING]
    american_tickers = [t for t in tickers if t not in EUROPEAN_UNDERLYING]

    expiry_rows: List[dict] = []
    quotes: List[pd.DataFrame] = []

    # 1. Free parity fits of the European expiries, and the day's curve.
    groups = []
    nodes = []
    for (ticker, root, expiry), group in chain[chain["ticker"].isin(european)].groupby(["ticker", "root", "expiry"]):
        settlement = SETTLEMENT.get(root)
        if settlement is None:
            expiry_rows.append(_expiry_row(day, ticker, expiry, root, "european", status="rejected",
                                           rejection_reason="unknown_root", n_quotes=0))
            continue
        ttm = years_between(valuation, settlement_time(expiry, settlement))
        if ttm <= 0:
            continue
        free = parity.fit(group)
        groups.append((ticker, root, expiry, group, ttm, free))
        if free is not None and np.isfinite(free.forward):
            nodes.append((ttm, free.discount, free.discount_se, free.dispersion / max(free.half_band, 1e-12)))
    curve = rates.from_parity(nodes, session)
    if curve is None:
        curve = rates.from_fred(inputs.fred)
    if curve is None:
        logger.warning("options-chain-iv %s: no discount curve (no European parity, no FRED): nothing computed", day)
        return pd.DataFrame(), _Day(day, pd.DataFrame(expiry_rows, columns=[c.name for c in EXPIRY_COLUMNS]))
    fallback = int(Flag.RATE_FALLBACK) if curve.source == "fred" else 0

    # 2. European expiries, refitted at the curve's DF.
    for ticker, root, expiry, group, ttm, free in groups:
        discount = float(curve.discount(ttm))
        underlying, scale = EUROPEAN_UNDERLYING[ticker]
        spot = closes.get(underlying)
        spot = spot * scale if spot is not None else None
        fitted = parity.fit(group, discount=discount)
        base = dict(ttm=ttm, discount=discount, discount_source=curve.source, implied_rate=-np.log(discount) / ttm,
                    spot=spot, forward_source="parity",
                    free_discount=free.discount if free is not None else None,
                    free_discount_se=free.discount_se if free is not None else None)
        if fitted is None or not np.isfinite(fitted.forward):
            expiry_rows.append(_expiry_row(day, ticker, expiry, root, "european", status="rejected",
                                           rejection_reason="too_few_pairs", n_quotes=0, **base))
            continue
        ratio = fitted.dispersion / max(fitted.half_band, 1e-12)
        reason = None
        if ratio > MAX_PARITY_DISPERSION:
            reason = "parity_dispersion"
        elif fitted.bound_violations > MAX_BOUND_VIOLATIONS * fitted.n_pairs:
            reason = "parity_bounds"
        exp = _Expiry(ttm, fitted.forward, discount, "european", "parity",
                      fallback | (int(Flag.EXPIRY_REJECTED) if reason else 0), fitted.outlier_strikes)
        rows = quote_rows(group, exp, stale[group.index].to_numpy())
        quotes.append(rows)
        expiry_rows.append(_expiry_row(
            day, ticker, expiry, root, "european", forward=fitted.forward,
            implied_yield=(base["implied_rate"] - np.log(fitted.forward / spot) / ttm) if spot else None,
            parity_forward=fitted.forward, parity_dispersion=fitted.dispersion, parity_half_band=fitted.half_band,
            n_pairs=fitted.n_pairs, n_bound_violations=fitted.bound_violations, n_quotes=len(rows),
            status="rejected" if reason else "accepted", rejection_reason=reason, **base,
        ))

    # 3. American expiries, at the curve's DF and the carry forward.
    dividend_dates: Dict[str, Optional[date]] = {}
    for (ticker, root, expiry), group in chain[chain["ticker"].isin(american_tickers)].groupby(["ticker", "root", "expiry"]):
        ttm = years_between(valuation, settlement_time(expiry, "pm"))
        if ttm <= 0:
            continue
        discount = float(curve.discount(ttm))
        rate = -np.log(discount) / ttm
        spot = closes.get(ticker)
        base = dict(ttm=ttm, discount=discount, discount_source=curve.source, implied_rate=rate, spot=spot,
                    forward_source="carry")
        if spot is None:
            expiry_rows.append(_expiry_row(day, ticker, expiry, root, "american", status="rejected",
                                           rejection_reason="no_spot", n_quotes=0, **base))
            continue
        carry, dividend_dates[ticker] = carry_of(ticker, inputs, spot, rate, expiry)
        forward = carry.forward(ttm)

        outliers, violations = _american_bounds(group, carry, discount, ttm)
        exp = _Expiry(ttm, forward, discount, "american", "carry", fallback, outliers, carry)
        rows = quote_rows(group, exp, stale[group.index].to_numpy(), band=AMERICAN_PARITY_BAND)
        diag = _american_diagnostics(rows, forward, discount)
        reason = None
        if diag["n_pairs"] < AMERICAN_MIN_PAIRS:
            reason = "too_few_pairs"
        elif diag["parity_dispersion"] > AMERICAN_MAX_DISPERSION * max(diag["parity_half_band"], 1e-12):
            reason = "parity_dispersion"
        if not rows.empty:
            stored = ((rows["flags"] & int(Flag.NOT_OTM)) == 0) | (rows["log_moneyness"].abs() < BOTH_SIDES_BAND)
            rows = rows[stored].reset_index(drop=True)
            if reason:
                rows["flags"] |= int(Flag.EXPIRY_REJECTED)
        quotes.append(rows)
        expiry_rows.append(_expiry_row(
            day, ticker, expiry, root, "american", forward=forward,
            implied_yield=rate - np.log(forward / spot) / ttm, n_quotes=len(rows), n_bound_violations=violations,
            status="rejected" if reason else "accepted", rejection_reason=reason, **diag, **base,
        ))

    frame = pd.concat([q for q in quotes if not q.empty], ignore_index=True) if quotes else pd.DataFrame()
    expiries = pd.DataFrame(expiry_rows, columns=[c.name for c in EXPIRY_COLUMNS])
    for column in EXPIRY_COLUMNS:
        if column.type == "INTEGER":
            expiries[column.name] = pd.to_numeric(expiries[column.name]).round().astype("Int64")
    loads = [
        {
            "ticker": ticker, "date": day, "method_version": METHOD_VERSION, "valuation_time": valuation.to_pydatetime(),
            "curve_source": curve.source, "curve_date": curve.as_of,
            "spot_date": session if (EUROPEAN_UNDERLYING.get(ticker, (ticker,))[0] in closes) else None,
            "dividend_date": dividend_dates.get(ticker),
        }
        for ticker in tickers
    ]
    return frame, _Day(day, expiries, loads)


def _american_bounds(group: pd.DataFrame, carry: american.Carry, discount: float, ttm: float):
    """The strikes whose pair breaks the American parity bounds,
    S exp(-qT) - PV(D) - K <= C - P <= S - K DF, beyond its bid/ask, and
    how many there are."""
    both = parity.pairs(group)
    if both.empty:
        return np.array([]), 0
    strike = both.index.to_numpy(dtype=float)
    lower = carry.spot * np.exp(-carry.dividend_yield * ttm) - carry.dividends_pv(ttm) - strike
    upper = carry.spot - strike * discount
    outside = ((both["ask_c"] - both["bid_p"]).to_numpy() < lower) | ((both["bid_c"] - both["ask_p"]).to_numpy() > upper)
    return strike[outside], int(outside.sum())


def _american_diagnostics(rows: pd.DataFrame, forward: float, discount: float) -> dict:
    """European parity on the de-Americanized prices near the money: per
    strike with both sides priced, the parity forward K + (C_eu - P_eu) / DF.
    Their median's gap from the carry forward measures the spot's asynchrony
    and any error in r or q; their dispersion, against their spreads, the
    expiry's consistency."""
    out = {"parity_forward": None, "carry_forward_gap": None, "parity_dispersion": None,
           "parity_half_band": None, "n_pairs": 0}
    if rows.empty:
        return out
    priced = rows[np.isfinite(rows["price_european"]) & rows["bid"].gt(0) & rows["ask"].gt(0)]
    calls = priced[priced["option_type"] == "call"].set_index("strike")
    puts = priced[priced["option_type"] == "put"].set_index("strike")
    columns = ["price_european", "bid", "ask"]
    both = calls[columns].add_suffix("_c").join(puts[columns].add_suffix("_p"), how="inner")
    out["n_pairs"] = len(both)
    if both.empty:
        return out
    strike = both.index.to_numpy(dtype=float)
    implied = strike + (both["price_european_c"] - both["price_european_p"]).to_numpy() / discount
    half = (((both["ask_c"] - both["bid_c"]) + (both["ask_p"] - both["bid_p"])) / 2.0).to_numpy() / discount
    center = float(np.median(implied))
    out["parity_forward"] = center
    out["carry_forward_gap"] = center - forward
    out["parity_dispersion"] = float(1.4826 * np.median(np.abs(implied - center)))
    out["parity_half_band"] = float(np.median(half))
    return out
