"""The order book capture: the event format, the reading of Bitstamp's
messages, the recorder and the verification of a day file.

The message sequences are the ones Bitstamp sent on BTC/USD on 11 October 2026
(ids shortened, fields trimmed to the ones read). The `network` test at the
bottom records the live feed for half a minute."""

import asyncio
import json
from datetime import date

import pytest

from data_ingest.capture.bitstamp import Instrument, Normaliser, Snapshot, to_int
from data_ingest.capture.book import Book
from data_ingest.capture.events import Event, EventType, Side, read_events, write_header
from data_ingest.capture.service import SECOND, Recorder, run
from data_ingest.capture.verify import verify
from data_ingest.capture.writer import DayWriter
from data_ingest.sources.capture_catalogue import CaptureCatalogue, scan

BTCUSD = Instrument("bitstamp", "btcusd", price_decimals=2, qty_decimals=8)
#: 2026-10-11 21:35:45 UTC, in microseconds.
T0 = 1_791_754_545_000_000
BUY, SELL = 0, 1


class Feed:
    """Builds `live_orders` messages, each naming the previous one."""

    def __init__(self):
        self.count = 0

    def message(self, event, order_id, side, price, amount, traded="0", at=T0):
        self.count += 1
        return {
            "event": f"order_{event}",
            "channel": "live_orders_btcusd",
            "event_id": f"e{self.count}",
            "pre_event_id": f"e{self.count - 1}",
            "data": {"id": order_id, "order_type": side, "price_str": price, "amount_str": amount,
                     "amount_traded": traded, "microtimestamp": str(at)},
        }


def snapshot(at=T0, bids=(("83400.00", "1.00000000", "11"),), asks=(("83410.00", "0.50000000", "12"),)):
    body = {"microtimestamp": str(at), "bids": [list(b) for b in bids], "asks": [list(a) for a in asks]}
    return Snapshot.parse(body, BTCUSD)


def loaded():
    normaliser = Normaliser(BTCUSD)
    normaliser.load(snapshot(), ts_recv=0)
    return normaliser


def shapes(events):
    return [(e.type.name, e.side.name, e.order_id, e.price, e.qty) for e in events]


def events_of(path):
    with open(path, "rb") as stream:
        return list(read_events(stream))


# ── the event format ─────────────────────────────────────────────────────────


def test_a_record_is_the_fifty_bytes_quant_microstructure_reads(tmp_path):
    event = Event(ts=1, ts_recv=2, seq=3, order_id=4, price=5, qty=6, type=EventType.TRADE, side=Side.SELL)
    packed = event.pack()
    assert len(packed) == 50
    # Six little-endian 64-bit fields, then the type and the side.
    assert packed == b"".join(n.to_bytes(8, "little") for n in (1, 2, 3, 4, 5, 6)) + bytes([2, 1])

    path = tmp_path / "day.qmev"
    with open(path, "wb") as out:
        write_header(out)
        out.write(packed)
    assert path.read_bytes()[:8] == b"QMEV\x01\x00\x00\x00"
    assert events_of(path) == [event]


def test_reading_refuses_what_is_not_a_whole_event_file(tmp_path):
    path = tmp_path / "day.qmev"
    path.write_bytes(b"NOPE\x01\x00\x00\x00")
    with pytest.raises(ValueError, match="not an event file"):
        events_of(path)
    path.write_bytes(b"QMEV\x02\x00\x00\x00")
    with pytest.raises(ValueError, match="version"):
        events_of(path)
    path.write_bytes(b"QMEV\x01\x00\x00\x00" + b"\x00" * 49)
    with pytest.raises(ValueError, match="truncated"):
        events_of(path)


# ── prices and sizes ─────────────────────────────────────────────────────────


def test_prices_and_sizes_become_integers_without_a_float():
    assert to_int("83407.09", 2) == 8340709
    assert to_int("83407", 2) == 8340700
    assert to_int("0.00006053", 8) == 6053
    assert to_int("0", 8) == 0
    assert to_int("83407.0900", 2) == 8340709
    with pytest.raises(ValueError):
        to_int("83407.095", 2)


def test_the_instrument_comes_from_the_markets_list():
    markets = [{"market_symbol": "ethusd", "counter_decimals": 1, "base_decimals": 8},
               {"market_symbol": "btcusd", "counter_decimals": 2, "base_decimals": 8}]
    instrument = Instrument.from_markets(markets, "btcusd")
    assert instrument == BTCUSD
    assert (instrument.tick_size, instrument.lot_size) == ("0.01", "0.00000001")
    with pytest.raises(KeyError):
        Instrument.from_markets(markets, "nopair")


# ── Bitstamp's messages ──────────────────────────────────────────────────────


def test_a_snapshot_loads_as_a_clear_then_every_order():
    normaliser = Normaliser(BTCUSD)
    events = normaliser.load(snapshot(at=T0 + 123), ts_recv=7)
    assert shapes(events) == [("CLEAR", "BUY", 0, 0, 0), ("ADD", "BUY", 11, 8340000, 100000000),
                              ("ADD", "SELL", 12, 8341000, 50000000)]
    assert all(e.ts == (T0 + 123) * 1000 and e.ts_recv == 7 for e in events)


def test_an_order_that_does_not_cross_rests_and_its_cancellation_removes_it():
    normaliser, feed = loaded(), Feed()
    created = normaliser.apply(feed.message("created", 20, BUY, "83405.00", "0.30000000", at=T0 + 2000), 1)
    assert shapes(created) == [("ADD", "BUY", 20, 8340500, 30000000)]
    assert created[0].ts == (T0 + 2000) * 1000
    cancelled = normaliser.apply(feed.message("deleted", 20, BUY, "83405.00", "0.30000000", at=T0 + 3000), 2)
    assert shapes(cancelled) == [("CANCEL", "BUY", 20, 8340500, 30000000)]
    assert 20 not in normaliser.book.orders and not normaliser.defects


def test_an_order_that_trades_on_arrival_gives_one_trade_and_never_rests():
    # A buy at 83438.00 takes 0.00006053 from the sell resting at 83410.00:
    # Bitstamp creates it, deletes it at the traded price, then changes the
    # resting order.
    normaliser, feed = loaded(), Feed()
    at = T0 + 2000
    events = []
    events += normaliser.apply(feed.message("created", 30, BUY, "83438.00", "0.00006053", at=at), 1)
    events += normaliser.apply(feed.message("deleted", 30, BUY, "83410.00", "0", "0.00006053", at=at), 1)
    events += normaliser.apply(feed.message("changed", 12, SELL, "83410.00", "0.49993947", "0.00006053", at=at), 1)
    # One Trade: on the resting order, at its price, signed by the aggressor.
    assert shapes(events) == [("TRADE", "BUY", 12, 8341000, 6053)]
    assert normaliser.book.orders[12] == (Side.SELL, 8341000, 49993947)
    assert 30 not in normaliser.book.orders and not normaliser.defects


def test_a_resting_order_filled_whole_is_a_trade_not_a_cancellation():
    normaliser, feed = loaded(), Feed()
    at = T0 + 2000
    normaliser.apply(feed.message("created", 31, SELL, "83300.00", "1.00000000", at=at), 1)
    normaliser.apply(feed.message("deleted", 31, SELL, "83400.00", "0", "1.00000000", at=at), 1)
    events = normaliser.apply(feed.message("deleted", 11, BUY, "83400.00", "0", "1.00000000", at=at), 1)
    assert shapes(events) == [("TRADE", "SELL", 11, 8340000, 100000000)]
    assert 11 not in normaliser.book.orders


def test_what_is_left_of_a_taker_rests_at_its_limit_when_the_next_order_arrives():
    # A buy of 0.8 at 83410.00 takes the 0.5 offered there; 0.3 is left.
    normaliser, feed = loaded(), Feed()
    at = T0 + 2000
    events = []
    events += normaliser.apply(feed.message("created", 32, BUY, "83410.00", "0.80000000", at=at), 1)
    events += normaliser.apply(feed.message("changed", 32, BUY, "83410.00", "0.30000000", "0.50000000", at=at), 1)
    events += normaliser.apply(feed.message("deleted", 12, SELL, "83410.00", "0", "0.50000000", at=at), 1)
    assert shapes(events) == [("TRADE", "BUY", 12, 8341000, 50000000)]
    following = normaliser.apply(feed.message("created", 33, SELL, "83500.00", "1.00000000", at=at), 2)
    assert shapes(following) == [("ADD", "BUY", 32, 8341000, 30000000), ("ADD", "SELL", 33, 8350000, 100000000)]
    assert not normaliser.book.crossed() and not normaliser.defects


def test_what_is_left_of_a_taker_that_is_cancelled_at_once_never_rests():
    # An immediate-or-cancel sell: two fills, then the rest deleted at its
    # limit price (the sequence of 21:36:23 UTC).
    normaliser = Normaliser(BTCUSD)
    normaliser.load(snapshot(bids=(("83407.08", "0.00058266", "11"), ("83407.08", "0.00049660", "13")), asks=()), 0)
    feed, at = Feed(), T0 + 2000
    events = []
    events += normaliser.apply(feed.message("created", 35, SELL, "83405.88", "0.06999994", at=at), 1)
    events += normaliser.apply(feed.message("changed", 35, SELL, "83407.08", "0.06941728", "0.00058266", at=at), 1)
    events += normaliser.apply(feed.message("deleted", 11, BUY, "83407.08", "0", "0.00058266", at=at), 1)
    events += normaliser.apply(feed.message("changed", 35, SELL, "83407.08", "0.06892068", "0.00049660", at=at), 1)
    events += normaliser.apply(feed.message("deleted", 13, BUY, "83407.08", "0", "0.00049660", at=at), 1)
    events += normaliser.apply(feed.message("deleted", 35, SELL, "83405.88", "0.06892068", at=at), 1)
    events += normaliser.flush(2)
    assert shapes(events) == [("TRADE", "SELL", 11, 8340708, 58266), ("TRADE", "SELL", 13, 8340708, 49660)]
    assert not normaliser.book.orders and not normaliser.defects


def test_a_message_about_an_order_never_seen_is_counted_and_changes_nothing():
    normaliser, feed = loaded(), Feed()
    assert normaliser.apply(feed.message("deleted", 99, BUY, "83000.00", "1.00000000", at=T0 + 2000), 1) == []
    assert normaliser.apply(feed.message("created", 11, BUY, "83400.00", "1.00000000", at=T0 + 2000), 1) == []
    assert normaliser.defects == {"unknown_order": 1, "duplicate_order": 1}


def test_messages_of_the_millisecond_of_the_snapshot_may_already_be_in_it():
    # The snapshot was taken at T0 + 0.4 ms; message times are cut to T0.
    normaliser = Normaliser(BTCUSD)
    normaliser.load(snapshot(at=T0 + 400), 0)
    feed = Feed()
    events = []
    # Already in the snapshot: the creation of 11, a fill that left 12 at 0.5,
    # the deletion of an order that is gone.
    events += normaliser.apply(feed.message("created", 11, BUY, "83400.00", "1.00000000", at=T0), 1)
    events += normaliser.apply(feed.message("changed", 12, SELL, "83410.00", "0.50000000", "0.10000000", at=T0), 1)
    events += normaliser.apply(feed.message("deleted", 98, BUY, "83000.00", "1.00000000", at=T0), 1)
    assert events == [] and not normaliser.defects
    # Not in it yet: applied.
    events = normaliser.apply(feed.message("created", 40, BUY, "83401.00", "0.20000000", at=T0), 1)
    assert shapes(events) == [("ADD", "BUY", 40, 8340100, 20000000)]
    # The next millisecond is strict again.
    normaliser.apply(feed.message("deleted", 97, BUY, "83000.00", "1.00000000", at=T0 + 1000), 1)
    assert normaliser.defects == {"unknown_order": 1}


def test_a_repriced_order_goes_to_the_back_of_its_new_price():
    normaliser, feed = loaded(), Feed()
    events = normaliser.apply(feed.message("changed", 11, BUY, "83401.00", "1.00000000", at=T0 + 2000), 1)
    assert shapes(events) == [("CANCEL", "BUY", 11, 8340000, 100000000), ("ADD", "BUY", 11, 8340100, 100000000)]


def test_the_events_of_a_normaliser_are_valid_for_a_strict_book():
    normaliser, feed = loaded(), Feed()
    at = T0 + 2000
    events = normaliser.dump(0)
    for message in (
        feed.message("created", 20, BUY, "83405.00", "0.30000000", at=at),
        feed.message("created", 32, BUY, "83410.00", "0.80000000", at=at),
        feed.message("changed", 32, BUY, "83410.00", "0.30000000", "0.50000000", at=at),
        feed.message("deleted", 12, SELL, "83410.00", "0", "0.50000000", at=at),
        feed.message("deleted", 20, BUY, "83405.00", "0.30000000", at=at + 1000),
    ):
        events += normaliser.apply(message, 1)
    replayed = Book()
    for event in events:
        replayed.apply(event)
    assert replayed.orders == normaliser.book.orders


# ── the recorder ─────────────────────────────────────────────────────────────


def recording(tmp_path, feed, check_interval=600.0):
    """A recorder that started from the default snapshot, taken at T0 + 5.4 ms
    and answered a second later: one message is from before, one from after."""
    recorder = Recorder(tmp_path, BTCUSD, now=T0 * 1000, check_interval=check_interval)
    recorder.on_message(feed.message("created", 11, BUY, "83400.00", "1.00000000", at=T0), T0 * 1000 + 30)
    recorder.on_message(feed.message("created", 20, BUY, "83405.00", "0.30000000", at=T0 + 10_000), T0 * 1000 + 40)
    recorder.on_snapshot(snapshot(at=T0 + 5400), ts_recv=T0 * 1000 + SECOND)
    return recorder


def test_a_recording_starts_from_a_snapshot_and_the_messages_that_followed_it(tmp_path):
    recorder = Recorder(tmp_path, BTCUSD, now=T0 * 1000)
    feed = Feed()
    assert not recorder.wants_snapshot(T0 * 1000 + 3 * SECOND)  # nothing buffered yet
    recorder.on_message(feed.message("created", 11, BUY, "83400.00", "1.00000000", at=T0), T0 * 1000 + 30)
    assert not recorder.wants_snapshot(T0 * 1000 + SECOND)  # the buffer must predate the snapshot
    assert recorder.wants_snapshot(T0 * 1000 + 3 * SECOND)
    recorder.on_message(feed.message("created", 20, BUY, "83405.00", "0.30000000", at=T0 + 10_000), T0 * 1000 + 40)
    recorder.on_snapshot(snapshot(at=T0 + 5400), ts_recv=T0 * 1000 + SECOND)
    assert recorder.live
    recorder.close()

    events = events_of(recorder.writer.path)
    assert recorder.writer.path == tmp_path / "bitstamp" / "btcusd" / "btcusd-2026-10-11.qmev"
    assert shapes(events) == [("CLEAR", "BUY", 0, 0, 0), ("ADD", "BUY", 11, 8340000, 100000000),
                              ("ADD", "SELL", 12, 8341000, 50000000), ("ADD", "BUY", 20, 8340500, 30000000)]
    assert [e.seq for e in events] == [1, 2, 3, 4]
    # Both clocks are kept: the exchange's, and when the message arrived.
    assert (events[3].ts, events[3].ts_recv) == ((T0 + 10_000) * 1000, T0 * 1000 + 40)

    sidecar = json.loads(recorder.writer.sidecar_path.read_text())
    assert (sidecar["kind"], sidecar["tick_size"], sidecar["lot_size"]) == ("l3", "0.01", "0.00000001")
    assert sidecar["events"] == 4 and sidecar["quality"]["syncs"] == 1 and "gaps" not in sidecar["quality"]


def test_a_snapshot_older_than_the_buffer_is_refused(tmp_path):
    recorder = Recorder(tmp_path, BTCUSD, now=T0 * 1000)
    recorder.on_message(Feed().message("created", 20, BUY, "83405.00", "0.30000000", at=T0 + 10_000), T0 * 1000)
    recorder.on_snapshot(snapshot(at=T0 + 5400), ts_recv=T0 * 1000 + SECOND)
    assert not recorder.live and recorder.writer.quality["snapshots_too_old"] == 1
    assert recorder.writer.events == 0


def test_a_message_that_does_not_follow_the_previous_one_is_a_hole(tmp_path):
    feed = Feed()
    recorder = recording(tmp_path, feed)
    feed.count += 1  # a message is lost
    recorder.on_message(feed.message("created", 21, BUY, "83406.00", "0.10000000", at=T0 + 20_000), T0 * 1000 + 50)
    assert not recorder.live and recorder.writer.quality["chain_breaks"] == 1
    # Nothing is written until a new snapshot; then the numbering skips one
    # and the stream goes on with a Clear and the book.
    recorder.on_message(feed.message("created", 22, BUY, "83407.00", "0.10000000", at=T0 + 3_000_000), T0 * 1000 + 60)
    later = snapshot(at=T0 + 2_500_400, bids=(("83400.00", "1.00000000", "11"), ("83406.00", "0.10000000", "21")))
    recorder.on_snapshot(later, ts_recv=T0 * 1000 + 4 * SECOND)
    recorder.close()

    events = events_of(recorder.writer.path)
    assert [e.seq for e in events] == [1, 2, 3, 4, 6, 7, 8, 9, 10]
    assert shapes(events[4:]) == [("CLEAR", "BUY", 0, 0, 0), ("ADD", "BUY", 11, 8340000, 100000000),
                                  ("ADD", "BUY", 21, 8340600, 10000000), ("ADD", "SELL", 12, 8341000, 50000000),
                                  ("ADD", "BUY", 22, 8340700, 10000000)]
    report = verify(recorder.writer.path)
    assert (report["gaps"], report["missing"], report["invalid_events"], report["ok"]) == (1, 1, 0, True)
    assert recorder.writer.quality["gaps"] == 1


def test_a_later_snapshot_that_matches_is_kept_as_a_checkpoint(tmp_path):
    feed = Feed()
    recorder = recording(tmp_path, feed, check_interval=5.0)
    assert not recorder.wants_snapshot(T0 * 1000 + 3 * SECOND)
    assert recorder.wants_snapshot(T0 * 1000 + 7 * SECOND)
    # Order 23 arrives after the snapshot was taken: the book has moved on,
    # and that order is left out of the comparison.
    recorder.on_message(feed.message("created", 23, BUY, "83300.00", "0.10000000", at=T0 + 6_500_000), T0 * 1000 + 70)
    published = snapshot(at=T0 + 6_000_400, bids=(("83400.00", "1.00000000", "11"), ("83405.00", "0.30000000", "20")))
    recorder.on_snapshot(published, ts_recv=T0 * 1000 + 7 * SECOND)
    recorder.on_message(feed.message("created", 24, BUY, "83301.00", "0.10000000", at=T0 + 7_500_000), T0 * 1000 + 80)
    recorder.close()
    assert recorder.live and recorder.writer.quality["checkpoints_ok"] == 1

    report = verify(recorder.writer.path)
    assert report["checkpoints"] == {"total": 1, "matched": 1, "failed": 0, "outside_file": 0, "wrong_orders": 0}
    assert report["ok"] and report["by_type"] == {"add": 5, "clear": 1}


def test_a_later_snapshot_that_differs_sends_the_recorder_back_to_the_start(tmp_path):
    feed = Feed()
    recorder = recording(tmp_path, feed, check_interval=5.0)
    # The exchange shows 0.9 on order 11, which no message has touched.
    published = snapshot(at=T0 + 6_000_400, bids=(("83400.00", "0.90000000", "11"), ("83405.00", "0.30000000", "20")))
    recorder.on_snapshot(published, ts_recv=T0 * 1000 + 7 * SECOND)
    assert not recorder.live
    assert recorder.writer.quality["checkpoints_failed"] == 1 and recorder.writer.quality["resyncs"] == 1
    recorder.on_message(feed.message("created", 24, BUY, "83301.00", "0.10000000", at=T0 + 7_500_000), T0 * 1000 + 80)
    recorder.close()

    # The replay finds the same: the file is wrong at that checkpoint.
    report = verify(recorder.writer.path)
    assert report["checkpoints"]["failed"] == 1 and report["checkpoints"]["wrong_orders"] == 1
    assert not report["ok"]


def test_a_new_day_starts_a_file_that_replays_on_its_own(tmp_path):
    feed = Feed()
    recorder = recording(tmp_path, feed)
    midnight = T0 + 3 * 3600 * 1_000_000  # 00:35:45 UTC, the next day
    recorder.on_message(feed.message("deleted", 20, BUY, "83405.00", "0.30000000", at=midnight), midnight * 1000 + 30)
    recorder.close()

    first = json.loads((tmp_path / "bitstamp" / "btcusd" / "btcusd-2026-10-11.json").read_text())
    assert first["closed"] is True and first["events"] == 4
    assert recorder.writer.day == date(2026, 10, 12)
    events = events_of(recorder.writer.path)
    assert [e.seq for e in events] == [1, 2, 3, 4, 5]
    assert shapes(events) == [("CLEAR", "BUY", 0, 0, 0), ("ADD", "BUY", 11, 8340000, 100000000),
                              ("ADD", "SELL", 12, 8341000, 50000000), ("ADD", "BUY", 20, 8340500, 30000000),
                              ("CANCEL", "BUY", 20, 8340500, 30000000)]
    assert verify(recorder.writer.path)["ok"]


def test_a_restart_resumes_the_file_of_the_day_after_a_hole(tmp_path):
    recording(tmp_path, Feed()).close()
    path = tmp_path / "bitstamp" / "btcusd" / "btcusd-2026-10-11.qmev"
    with open(path, "ab") as out:
        out.write(b"\x00" * 17)  # a record cut short by a crash

    feed = Feed()
    recorder = Recorder(tmp_path, BTCUSD, now=T0 * 1000 + 60 * SECOND)
    assert (recorder.writer.events, recorder.writer.seq) == (4, 4)
    recorder.on_message(feed.message("created", 25, BUY, "83300.00", "0.10000000", at=T0 + 60_000_000),
                        T0 * 1000 + 60 * SECOND)
    recorder.on_snapshot(snapshot(at=T0 + 61_000_400), ts_recv=T0 * 1000 + 62 * SECOND)
    recorder.close()

    events = events_of(path)
    assert [e.seq for e in events] == [1, 2, 3, 4, 6, 7, 8]
    assert events[4].type is EventType.CLEAR
    sidecar = json.loads(recorder.writer.sidecar_path.read_text())
    assert sidecar["quality"]["gaps"] == 1 and sidecar["quality"]["syncs"] == 2


def test_the_clock_is_measured_on_every_message(tmp_path):
    recorder = recording(tmp_path, Feed())
    quality = recorder.writer.quality
    # Message 11 was dropped (before the snapshot); message 20 arrived 40 ns
    # after T0 with an exchange time of T0 + 10 ms.
    assert quality["latency_count"] == 1
    assert quality["latency_ns_min"] == quality["latency_ns_max"] == 40 - 10_000_000
    assert quality["negative_latency"] == 1


# ── verification ─────────────────────────────────────────────────────────────


def test_verify_counts_the_events_a_strict_book_would_refuse(tmp_path):
    writer = DayWriter(tmp_path, BTCUSD, date(2026, 10, 11))
    writer.write([
        Event(1, 1, 0, 0, 0, 0, EventType.CLEAR, Side.BUY),
        Event(2, 2, 0, 11, 8340000, 100, EventType.ADD, Side.BUY),
        Event(3, 3, 0, 11, 8340000, 100, EventType.ADD, Side.BUY),      # already resting
        Event(4, 4, 0, 11, 8340000, 150, EventType.CANCEL, Side.BUY),   # more than what rests
        Event(5, 5, 0, 12, 8339000, 100, EventType.ADD, Side.SELL),     # crosses the bid
    ])
    writer.close()
    report = verify(writer.path)
    assert (report["events"], report["invalid_events"], report["crossed"], report["ok"]) == (5, 2, 1, False)


def test_verify_refuses_a_file_that_is_not_an_event_file(tmp_path):
    path = tmp_path / "day.qmev"
    path.write_bytes(b"not an event file")
    with pytest.raises(ValueError):
        verify(path)


# ── the catalogue ────────────────────────────────────────────────────────────


def test_the_catalogue_describes_each_day_file_from_its_sidecar(tmp_path):
    recorder = recording(tmp_path, Feed())
    recorder.close()
    rows = scan(tmp_path, since=None)
    assert len(rows) == 1
    row = dict(zip(CaptureCatalogue.table.column_names, rows[0]))
    assert (row["venue"], row["pair"], row["date"], row["kind"]) == ("bitstamp", "btcusd", date(2026, 10, 11), "l3")
    assert row["path"] == "bitstamp/btcusd/btcusd-2026-10-11.qmev"
    assert (row["price_decimals"], row["qty_decimals"]) == (2, 8)
    assert (row["events"], row["bytes"], row["gaps"], row["defects"]) == (4, 8 + 4 * 50, 0, 0)
    assert row["first_ts_recv"].isoformat().startswith("2026-10-11T21:35:46")
    assert row["closed"] is False
    assert scan(tmp_path, since=date(2026, 10, 12)) == []


def test_the_catalogue_source_reads_the_capture_directory(tmp_path, monkeypatch):
    from data_ingest.core.source import Window
    from data_ingest.sources import capture_catalogue

    recording(tmp_path, Feed()).close()
    (tmp_path / "bitstamp" / "btcusd" / "broken.json").write_text("{")
    monkeypatch.setattr(capture_catalogue, "CAPTURE_DIR", tmp_path)
    frame = CaptureCatalogue().fetch(Window.everything())
    assert list(frame.columns) == CaptureCatalogue.table.column_names
    assert len(frame) == 1 and frame.iloc[0]["events"] == 4
    monkeypatch.setattr(capture_catalogue, "CAPTURE_DIR", tmp_path / "nothing-here")
    assert CaptureCatalogue().fetch(Window.everything()).empty


# ── live ─────────────────────────────────────────────────────────────────────


@pytest.mark.network
def test_live_feed_replays_to_the_book_the_exchange_publishes(tmp_path):
    async def record():
        try:
            async with asyncio.timeout(35):
                await run(tmp_path, "btcusd", check_interval=8.0)
        except TimeoutError:
            pass

    asyncio.run(record())
    (path,) = tmp_path.glob("bitstamp/btcusd/*.qmev")
    report = verify(path)
    assert report["events"] > 5000 and report["invalid_events"] == 0
    assert report["checkpoints"]["matched"] >= 1 and report["checkpoints"]["failed"] == 0
    sidecar = json.loads(path.with_suffix(".json").read_text())
    assert not sidecar["quality"].get("unknown_order") and not sidecar["quality"].get("duplicate_order")
