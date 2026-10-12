"""The capture service: a connection that stays open, unlike every source of
this repository, which are scheduled runs.

    ingest capture --pair btcusd,nearusd    record these pairs until stopped

`Recorder` is the logic of one pair, without any network: it is given messages
and snapshots and writes events. `run` is the shell around the recorders: for
each pair it holds a WebSocket, asks for snapshots and reconnects, and now and
then it compresses the days that are over and makes room (`housekeeping.py`).

A recording starts, and restarts after every hole, the same way: messages are
buffered, a snapshot of the whole book is fetched, the book is loaded from it
and the buffered messages that came after it are applied. From then on every
message is applied as it arrives. Three things send the recorder back to that
start, each leaving a hole in the numbering of the file:

- the connection drops, or the exchange goes silent;
- a message does not name the previous one as its predecessor;
- a later snapshot does not match the book (they are compared every
  `check_interval`, and kept as checkpoints for `verify`).

Nothing here touches Postgres: the capture must go on when the database is
down. The catalogue is filled from the files (`sources/capture_catalogue.py`).
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import deque
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Callable, Deque, List, Optional, Sequence, Tuple

import requests

from .bitstamp import ORDER_EVENTS, REST_URL, WS_URL, Instrument, Normaliser, Snapshot, message_time_us
from .housekeeping import tidy
from .writer import DayWriter

logger = logging.getLogger("data_ingest.capture")

SECOND = 1_000_000_000
#: How long messages are buffered before the snapshot is asked for. A snapshot
#: is older than its request (half a second, observed), and the buffer must
#: start before it.
SNAPSHOT_DELAY = 2 * SECOND
#: Minimum time between two snapshot requests.
SNAPSHOT_SPACING = 2 * SECOND
#: Orders touched over this long are remembered, to compare a snapshot with a
#: book that has moved on since. Older snapshots are not compared.
RECENT = 30 * SECOND
#: The pairs recorded send several messages a second at their quietest: this
#: long without one is a dead connection.
SILENCE = 30 * SECOND
#: Time between two rounds of housekeeping, in seconds.
TIDY_EVERY = 600.0
FLUSH_EVERY = 1 * SECOND
SAVE_EVERY = 15 * SECOND


def utc_day(ts: int) -> date:
    return datetime.fromtimestamp(ts / SECOND, tz=timezone.utc).date()


class Recorder:
    """Messages and snapshots in, day files out."""

    def __init__(self, root: Path, instrument: Instrument, now: int, check_interval: float = 600.0):
        self.root = root
        self.instrument = instrument
        self.check_interval = int(check_interval * SECOND)
        self.normaliser = Normaliser(instrument)
        self.live = False
        self.writer = DayWriter(root, instrument, utc_day(now))
        self.normaliser.defects = self.writer.quality
        self._buffer: List[Tuple[int, dict]] = []
        self._last_event_id: Optional[str] = None
        #: (message time in microseconds, order id) of the last messages applied.
        self._recent: Deque[Tuple[int, int]] = deque()
        self._next_check = 0
        self._last_snapshot_request = 0
        self._flushed = self._saved = now

    # ---------------------------------------------------------------- inputs

    def on_message(self, message: dict, ts_recv: int) -> None:
        """One `order_created`, `order_changed` or `order_deleted`."""
        previous = message.get("pre_event_id")
        if self._last_event_id is not None and previous is not None and previous != self._last_event_id:
            logger.warning("Hole in the feed: %s does not follow %s", message.get("event_id"), self._last_event_id)
            self.interrupt("chain_breaks")
        self._last_event_id = message.get("event_id")
        if self.live:
            self._apply(message, ts_recv)
        else:
            self._buffer.append((ts_recv, message))

    def wants_snapshot(self, now: int) -> bool:
        if now - self._last_snapshot_request < SNAPSHOT_SPACING:
            return False
        if self.live:
            return now >= self._next_check
        return bool(self._buffer) and now - self._buffer[0][0] >= SNAPSHOT_DELAY

    def snapshot_requested(self, now: int) -> None:
        self._last_snapshot_request = now

    def on_snapshot(self, snapshot: Snapshot, ts_recv: int) -> None:
        if self.live:
            self._check(snapshot, ts_recv)
        else:
            self._start(snapshot, ts_recv)

    def interrupt(self, reason: str) -> None:
        """The feed was cut, or the book is not to be trusted any more: back
        to buffering until the next snapshot."""
        self.writer.quality[reason] += 1
        self.live = False
        self._buffer = []
        self._last_event_id = None
        self._recent.clear()

    def tick(self, now: int) -> None:
        if now - self._saved >= SAVE_EVERY:
            self.writer.save()
            self._saved = self._flushed = now
        elif now - self._flushed >= FLUSH_EVERY:
            self.writer.flush()
            self._flushed = now

    def close(self) -> None:
        self.writer.close()

    # ------------------------------------------------------------- internals

    def _start(self, snapshot: Snapshot, ts_recv: int) -> None:
        first = snapshot.first_message_us
        if not self._buffer or message_time_us(self._buffer[0][1]) > first:
            # Messages between the snapshot and the buffer would be missing.
            self.writer.quality["snapshots_too_old"] += 1
            return
        self._roll(ts_recv, dump=False)
        self.writer.gap()
        self.writer.write(self.normaliser.load(snapshot, ts_recv))
        self.live = True
        buffered, self._buffer = self._buffer, []
        for received, message in buffered:
            if message_time_us(message) >= first:
                self._apply(message, received)
        self._next_check = ts_recv + self.check_interval
        self.writer.quality["syncs"] += 1
        logger.info("Recording %s from a book of %d orders", self.instrument.pair, len(snapshot.orders))

    def _check(self, snapshot: Snapshot, ts_recv: int) -> None:
        """Compares the book with a snapshot the exchange published a moment
        ago. Orders touched since then cannot be compared, the others must be
        identical."""
        self._next_check = ts_recv + self.check_interval
        first = snapshot.first_message_us
        if ts_recv - first * 1000 > RECENT:
            self.writer.quality["checkpoints_skipped"] += 1
            return
        self.writer.checkpoint(snapshot)
        touched = {order_id for at, order_id in self._recent if at >= first}
        want, got = snapshot.as_orders(), self.normaliser.book.orders
        wrong = [i for i in want.keys() | got.keys() if i not in touched and want.get(i) != got.get(i)]
        if not wrong:
            self.writer.quality["checkpoints_ok"] += 1
            return
        logger.warning("The book differs from the exchange's on %d orders (first: %s, exchange %s, here %s)",
                       len(wrong), wrong[0], want.get(wrong[0]), got.get(wrong[0]))
        self.writer.quality["checkpoints_failed"] += 1
        self.interrupt("resyncs")

    def _apply(self, message: dict, ts_recv: int) -> None:
        self._roll(ts_recv, dump=True)
        events = self.normaliser.apply(message, ts_recv)
        self.writer.write(events)
        at = message_time_us(message)
        self._recent.append((at, int(message["data"]["id"])))
        horizon = at - RECENT // 1000
        while self._recent[0][0] < horizon:
            self._recent.popleft()
        self._clock(ts_recv - at * 1000)

    def _roll(self, ts_recv: int, dump: bool) -> None:
        """Moves to the file of a new day, which starts with the whole book."""
        day = utc_day(ts_recv)
        if day <= self.writer.day:
            return
        self.writer.close(closed=True)
        self.writer = DayWriter(self.root, self.instrument, day)
        self.normaliser.defects = self.writer.quality
        if dump:
            self.writer.write(self.normaliser.dump(ts_recv))
        logger.info("New day: %s", self.writer.path)

    def _clock(self, latency: int) -> None:
        """Receive time minus exchange time. The exchange's is cut to the
        millisecond, so up to a millisecond of it is not latency."""
        quality = self.writer.quality
        quality["latency_count"] += 1
        quality["latency_ns_sum"] += latency
        if latency > quality.get("latency_ns_max", latency - 1):
            quality["latency_ns_max"] = latency
        if latency < quality.get("latency_ns_min", latency + 1):
            quality["latency_ns_min"] = latency
        if latency < 0:
            quality["negative_latency"] += 1


# ------------------------------------------------------------------- network


def fetch_instrument(pair: str) -> Instrument:
    response = requests.get(f"{REST_URL}/markets/", timeout=20)
    response.raise_for_status()
    return Instrument.from_markets(response.json(), pair)


def fetch_snapshot(instrument: Instrument) -> Tuple[Snapshot, int]:
    """The whole book, order by order, and when the answer arrived."""
    response = requests.get(f"{REST_URL}/order_book/{instrument.pair}/", params={"group": 2}, timeout=20)
    response.raise_for_status()
    return Snapshot.parse(response.json(), instrument), time.time_ns()


async def _session(recorder: Recorder, connect: Callable, snapshot: Callable) -> None:
    """One connection, until it fails."""
    pending: Optional[asyncio.Task] = None
    # A short closing handshake: when stopping, the files are closed after it.
    async with connect(WS_URL, close_timeout=2) as ws:
        channel = f"live_orders_{recorder.instrument.pair}"
        await ws.send(json.dumps({"event": "bts:subscribe", "data": {"channel": channel}}))
        heard = time.time_ns()
        try:
            while True:
                # Not asyncio.wait_for: on Python 3.11 it swallows a cancellation
                # that lands as a message arrives, and the service never stops.
                try:
                    async with asyncio.timeout(1.0):
                        raw = await ws.recv()
                except TimeoutError:
                    raw = None
                now = time.time_ns()
                if raw is not None:
                    message = json.loads(raw)
                    name = message.get("event")
                    if name in ORDER_EVENTS:
                        recorder.on_message(message, now)
                        heard = now
                    elif name in ("bts:request_reconnect", "bts:error"):
                        raise ConnectionError(f"the exchange sent {raw!r}")
                if now - heard > SILENCE:
                    raise ConnectionError("the feed went silent")
                if pending is not None and pending.done():
                    done, pending = pending, None
                    try:
                        book, received = done.result()
                    except Exception as exc:
                        logger.warning("Snapshot failed: %s", exc)
                        recorder.writer.quality["snapshot_errors"] += 1
                    else:
                        recorder.on_snapshot(book, received)
                if pending is None and recorder.wants_snapshot(now):
                    recorder.snapshot_requested(now)
                    pending = asyncio.create_task(asyncio.to_thread(snapshot, recorder.instrument))
                recorder.tick(now)
        except Exception:
            recorder.interrupt("disconnections")
            raise
        finally:
            if pending is not None:
                pending.cancel()


async def _record(recorder: Recorder, connect: Callable) -> None:
    """One pair, reconnecting until cancelled."""
    wait = 1.0
    while True:
        started = time.monotonic()
        try:
            await _session(recorder, connect, fetch_snapshot)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("%s: connection lost (%s), reconnecting in %.0f s", recorder.instrument.pair, exc, wait)
        # Back off while it keeps failing at once; start over after a session that held.
        wait = 1.0 if time.monotonic() - started > 60 else min(wait * 2, 60.0)
        await asyncio.sleep(wait)


async def _housekeep(root: Path, recorders: Sequence[Recorder], max_bytes: int) -> None:
    """Compresses the days that are over and keeps the directory within its
    budget, in a thread: compressing a day takes minutes, the feed does not
    wait."""
    while True:
        busy = {recorder.writer.path for recorder in recorders}
        try:
            await asyncio.to_thread(tidy, root, utc_day(time.time_ns()), busy, max_bytes)
        except Exception:
            logger.exception("Housekeeping failed, the recording goes on")
        await asyncio.sleep(TIDY_EVERY)


async def run(root: Path, pairs: Sequence[str] = ("btcusd",), check_interval: float = 600.0,
              max_bytes: int = 0) -> None:
    """Records until cancelled. `max_bytes` is the budget of the whole
    directory (0: nothing is ever removed)."""
    from websockets.asyncio.client import connect

    recorders: List[Recorder] = []
    try:
        for pair in pairs:
            instrument = await asyncio.to_thread(fetch_instrument, pair)
            recorder = Recorder(root, instrument, time.time_ns(), check_interval)
            recorders.append(recorder)
            logger.info("Capturing %s %s into %s (tick %s, lot %s)", instrument.venue, pair,
                        recorder.writer.path.parent, instrument.tick_size, instrument.lot_size)
        await asyncio.gather(*(_record(recorder, connect) for recorder in recorders),
                             _housekeep(root, recorders, max_bytes))
    finally:
        for recorder in recorders:
            recorder.close()
