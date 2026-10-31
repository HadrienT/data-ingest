# Order book capture

`ingest capture` records Bitstamp's public order-by-order feed: every order
created, changed and deleted, with its id. No account, no key. It is the one
long-running service of this repository (the `capture` service of
`docker-compose.yml`), and it writes files, not rows: the consumer,
`quant-microstructure`, replays a whole day in order, which a file does
without a query (its ADR-015).

**One pair** is recorded, BTC/USD. The service takes several
(`CAPTURE_PAIRS` in `.env`, comma-separated, each in its own files), and a
second one was looked for on 12 October 2026, to have one pair whose spread is
pinned to the tick and one whose tick is no constraint:

| | BTC/USD | NEAR/USD |
|---|---|---|
| tick | 0.01 on a price of 83,400 | 0.00001 on a price of 5.84 |
| spread, median | 1 tick | 427 ticks |
| time spent at a spread of one tick | 98.5 % | never |
| orders resting at each best price | 5.6 | 1.0 |
| events a second | 135 | 14 |
| trades a minute | 26 | 0.1 |

BTC/USD is the first kind: orders queue at the best price and the place in
the queue is what matters. So are the other large pairs (ETH, SOL, XRP, DOGE),
at one tick 70 to 90 % of the time. The pairs of the second kind, like
NEAR/USD, hardly trade: a book without trades is not worth recording yet. To
be added the day a study needs it; what was not recorded cannot be had later.

XRP/USD was tried and put aside: in twenty minutes its recorded book differed
once from the exchange's (one order, by its quantity), and messages about
orders the recorder did not know were counted, which did not happen on the
six other pairs tried. Something in its messages is not read correctly yet.

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
ingest capture                                    # record BTC/USD until stopped
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
compressed, plus 16 MB of checkpoints. About 0.15 GB a day in all, so the
default budget holds some 270 days. A full day
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
