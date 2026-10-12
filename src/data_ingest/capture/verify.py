"""Replays a day file and checks it against what the exchange published.

    ingest capture-verify <root>/bitstamp/btcusd/btcusd-2026-10-12.qmev.xz

The file, compressed or not, is read the way `quant-microstructure` will read it: every event is
applied to a strict order-by-order book. The report says

- whether every event was valid for that book (an Add of an id already
  resting, or a reduction of more than what rests, makes the C++ reader throw);
- how many holes the numbering has, and how many times the book was crossed;
- at each checkpoint, whether the replayed book is the one the exchange
  published. Event times are cut to the millisecond and a snapshot falls
  inside one, so the orders touched during that millisecond are left out of
  the comparison; every other order must be identical, id, side, price and
  quantity.
"""

from __future__ import annotations

import gzip
import json
from collections import Counter
from pathlib import Path
from typing import Dict, List, Tuple

from .book import Book
from .events import RECORD, EventType, Side
from .files import CHECKPOINTS, record_chunks, stem_of, with_suffix

MILLISECOND = 1_000_000


def read_checkpoints(path: Path) -> List[Tuple[int, Dict[int, Tuple[int, int, int]]]]:
    """(time in microseconds, {order id: (side, price, qty)}) per checkpoint."""
    if not path.exists():
        return []
    checkpoints = []
    with gzip.open(path, "rt") as lines:
        for line in lines:
            record = json.loads(line)
            orders = {order_id: (side, price, qty) for order_id, side, price, qty in record["orders"] if qty > 0}
            checkpoints.append((record["time_us"], orders))
    return sorted(checkpoints, key=lambda checkpoint: checkpoint[0])


def verify(path: Path) -> dict:
    """The report of one day file. Raises ValueError if it is not one."""
    checkpoints = read_checkpoints(with_suffix(stem_of(path), CHECKPOINTS))
    book = Book()
    types: Counter = Counter()
    report = {"events": 0, "invalid_events": 0, "gaps": 0, "missing": 0, "out_of_order": 0, "crossed": 0,
              "time_regressions": 0, "first_ts": None, "last_ts": None}
    compared = {"total": len(checkpoints), "matched": 0, "failed": 0, "outside_file": 0, "wrong_orders": 0}

    # The checkpoint being compared: the book as it stood before the
    # millisecond of the snapshot, and the orders touched during it.
    index = 0
    before = None
    touched: set = set()

    def conclude() -> None:
        nonlocal before, index
        want = checkpoints[index][1]
        wrong = sum(1 for i in want.keys() | before.keys() if i not in touched and want.get(i) != before.get(i))
        compared["matched" if not wrong else "failed"] += 1
        compared["wrong_orders"] += wrong
        before = None
        touched.clear()
        index += 1

    last_seq = None
    last_ts = None
    add, cancel, trade, clear = EventType.ADD, EventType.CANCEL, EventType.TRADE, EventType.CLEAR
    for chunk in record_chunks(path):
        for ts, _ts_recv, seq, order_id, price, qty, kind, side in RECORD.iter_unpack(chunk):
            if last_seq is None:
                report["first_ts"] = ts
                # A snapshot from before the file belongs to the previous day's.
                while index < len(checkpoints) and checkpoints[index][0] * 1000 < ts:
                    compared["outside_file"] += 1
                    index += 1
            elif seq != last_seq + 1:
                if seq > last_seq:
                    report["gaps"] += 1
                    report["missing"] += seq - last_seq - 1
                else:
                    report["out_of_order"] += 1
            last_seq = seq
            if last_ts is not None and ts < last_ts:
                report["time_regressions"] += 1
            last_ts = ts

            while index < len(checkpoints):
                start = checkpoints[index][0] // 1000 * MILLISECOND
                if before is None and ts >= start:
                    before = dict(book.orders)
                if before is not None and ts >= start + MILLISECOND:
                    conclude()
                    continue
                break
            if before is not None:
                touched.add(order_id)

            types[kind] += 1
            try:
                if kind == add:
                    book.add(order_id, Side(side), price, qty)
                    if book.crossed():
                        report["crossed"] += 1
                elif kind == cancel or kind == trade:
                    book.reduce(order_id, qty)
                elif kind == clear:
                    book.clear()
                else:
                    raise ValueError("not an event of an order-by-order stream")
            except ValueError:
                report["invalid_events"] += 1
    # No event after these snapshots: the book they describe is the last one.
    while last_seq is not None and index < len(checkpoints):
        if before is None:
            before = dict(book.orders)
        conclude()
    compared["outside_file"] += len(checkpoints) - index

    report["events"] = sum(types.values())
    report["last_ts"] = last_ts
    report["by_type"] = {EventType(kind).name.lower(): count for kind, count in sorted(types.items())}
    report["final_orders"] = len(book.orders)
    report["checkpoints"] = compared
    report["ok"] = not report["invalid_events"] and not report["out_of_order"] and not compared["failed"]
    return report


def format_report(path: Path, report: dict) -> str:
    compared = report["checkpoints"]
    events = report["events"]
    by_type = ", ".join(f"{count} {name}" for name, count in report["by_type"].items())
    rate = report["gaps"] / events if events else 0.0
    return "\n".join([
        f"{path.name}: {events} events ({by_type})",
        f"  valid for a strict book: {'yes' if not report['invalid_events'] else str(report['invalid_events']) + ' invalid events'}",
        f"  holes: {report['gaps']} ({report['missing']} numbers skipped, {rate:.2e} per event)",
        f"  crossed book after an Add: {report['crossed']}",
        f"  time going backwards: {report['time_regressions']}",
        f"  checkpoints: {compared['matched']} matched, {compared['failed']} failed"
        f" ({compared['wrong_orders']} wrong orders), {compared['outside_file']} outside the file",
        f"  => {'OK' if report['ok'] else 'FAILED'}",
    ])
