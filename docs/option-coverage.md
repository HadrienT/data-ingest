# Option chain coverage: SPX, XSP and SPY on yfinance

quant-modeling's WP 31 (decision D2) prices S&P 500 products on a European
surface, from either SPX or XSP. This note records how much of each chain
yfinance serves, so that the choice rests on a measurement.

Measured with `scripts/measure_option_coverage.py` on the chain captured on
2026-11-26. That day was Thanksgiving, so the chain holds the 2026-11-25
closing quotes. S is the S&P 500 close (`^GSPC`), a tenth of it for XSP, and
SPY's close for SPY.

## Totals

| Ticker | Quotes | Two-sided (bid > 0, ask > 0) | Roots | Expiries | Last expiry |
|---|---|---|---|---|---|
| `^SPX` | 22,268 | 21,047 | SPX 19, SPXW 36, SPXO 2 | 51 dates | 2031-12-19 |
| `^XSP` | 9,242 | 8,134 | XSP | 41 | 2028-12-15 |
| `SPY` | 9,921 | 9,183 | SPY | 29 | 2029-01-19 |

## What each chain covers

**SPX (AM-settled monthlies)**
- Strikes:
  - **Monthlies to 2027-03** have 580–650 two-sided quotes each, every 5 points near the money, from K/S 0.03 to 1.6–1.9.
  - **Monthlies beyond 2027-03** have 250–440 quotes each, every 25 points; 2027-12-17 is spaced every 50.
- Spread: the median ATM relative spread (|ln K/S| < 0.02) is **0.6–1.1 %** to 2028-12.
- The three expiries from 2029-12 are thin:
  - 148, 77 and 20 two-sided quotes;
  - ATM spreads of 11 %, 33 % and 58 %.

**SPXW (PM-settled)**
- Strikes:
  - every 5 points on the busiest expiries (Fridays, month and quarter ends), 10–25 points on the others;
  - K/S from 0.3–0.5 up to 1.2–1.5, so no deep wings.
- Spread: the median ATM relative spread is **0.3–0.7 %**, the tightest of the three tickers.

**Mixed roots on a monthly date**
- On a third Friday (2026-12-18, 2027-01-15), yfinance returns **one contract per strike**, SPX at some strikes and SPXW at others. SPX gets 352 of the calls on 2026-12-18 and SPXW 189, with no strike carrying both.
- Each root is therefore a partial strike grid on that date. They have different settlement times, so `options-chain-iv` fits each root's parity separately.

**SPXO**
- 2 expiries, about 60 quotes each.
- Not a standard Cboe root as far as this measurement goes. It is stored like the others and left out of the IV computation until its settlement is known.

**XSP**
- Strikes:
  - every 1 point near the money;
  - 100–400 two-sided quotes per expiry;
  - K/S 0.6–1.3 on most expiries, 0.07–2.06 on the long-dated monthlies.
- Spread:
  - the median ATM relative spread is **1.5–3 %**, three to five times SPX's;
  - in dollars it stays tight, the option being a tenth of SPX's size.
- Expiries run to 2028-12.

**SPY**
- Strikes:
  - every 1 point near the money on the near expiries, every 5 on the long ones;
  - K/S 0.07–2.07 on the long-dated monthlies.
- Spread: the median ATM relative spread is 0.6–1.5 % on the near expiries, 2–3 % from 2027-09.

## Comparison with Cboe's listing

**Not done.** Cboe's delayed-quotes API (`cdn.cboe.com/api/global/delayed_quotes/options/_SPX.json`) answers 403 to automated clients, so it cannot be run from here. It is to be repeated by hand, on a regular trading day, against Cboe's own list of strikes.

Without it, what can be said:
- **Monthly SPX strikes:** the issue's "56 calls on a one-month expiry" does not recur. The SPX monthlies of the next four months carry about 325 calls each, every 5 points.
- **SPXW strikes:** they stop at K/S ≈ 0.3–0.5 on the downside, whereas Cboe lists SPXW puts further out. yfinance's SPXW strike set looks truncated in the deep wings.

## Reading for D2

**SPX as the primary European surface:**
- **Spreads:** relative spreads are three to five times tighter than XSP's.
- **Wings:** strikes go far into the wings on the monthlies.
- **Maturities:** usable spreads to 2028-12, as on XSP, plus thin quotes out to 2031-12.

**XSP** has strikes at SPY's scale and stays useful as a second European chain:
- a second parity estimate of the discount factor;
- a check on SPX on the same day.

This reading is from a single day, a holiday's closing quotes. It should be repeated on a few regular trading days before D2 is closed.
