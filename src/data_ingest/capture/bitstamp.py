"""Bitstamp's public order-by-order feed, turned into events.

Bitstamp publishes every order of a pair without an account: the WebSocket
channel `live_orders_<pair>` sends `order_created`, `order_changed` and
`order_deleted`, and `GET /api/v2/order_book/<pair>/?group=2` returns the whole
book with order ids. What follows was read off the feed (11 October 2026), not
off the documentation, which says little:

- An order that trades on arrival is announced like any other: created at its
  limit price, then changed or deleted in the messages that follow, this time
  with the price it **traded** at. It never rested, so it is kept aside (a
  "taker") and gives no event; what is left of it when the next order arrives
  rests and becomes an Add.
- The order it hit is changed (or deleted, when nothing is left) with
  `amount_traded` set to the size of that one fill: that is the Trade.
- `amount` is what is left of the order after the message.
- Every message carries an `event_id` and the `pre_event_id` of the message
  before it: a mismatch is a hole in the feed.
- Times are cut to the millisecond (`microtimestamp` ends in 000), and several
  unrelated messages share one. A snapshot carries a true microsecond time and
  is about half a second old when it is answered.
- Within a price, a snapshot lists orders by increasing id, oldest first.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from typing import Dict, Iterable, List, Tuple

from .book import Book
from .events import Event, EventType, Side

VENUE = "bitstamp"
WS_URL = "wss://ws.bitstamp.net"
REST_URL = "https://www.bitstamp.net/api/v2"

ORDER_EVENTS = ("order_created", "order_changed", "order_deleted")


@dataclass(frozen=True)
class Instrument:
    """What turns the integers of a file back into prices and sizes."""

    venue: str
    pair: str
    price_decimals: int
    qty_decimals: int

    @property
    def tick_size(self) -> str:
        return _unit(self.price_decimals)

    @property
    def lot_size(self) -> str:
        return _unit(self.qty_decimals)

    @classmethod
    def from_markets(cls, markets: Iterable[dict], pair: str) -> "Instrument":
        """From the answer of `GET /api/v2/markets/`."""
        for market in markets:
            if market.get("market_symbol") == pair:
                return cls(VENUE, pair, int(market["counter_decimals"]), int(market["base_decimals"]))
        raise KeyError(f"Bitstamp lists no market {pair!r}")


def _unit(decimals: int) -> str:
    return "1" if decimals == 0 else "0." + "0" * (decimals - 1) + "1"


def to_int(text: str, decimals: int) -> int:
    """'83407.09' with two decimals is 8340709. Exact: a price never goes
    through a float. Raises ValueError on more decimals than the instrument
    has, which would mean the tick changed under the capture."""
    whole, _, fraction = text.partition(".")
    if len(fraction) > decimals:
        if fraction[decimals:].strip("0"):
            raise ValueError(f"{text!r} has more than {decimals} decimals")
        fraction = fraction[:decimals]
    return int(whole + fraction.ljust(decimals, "0"))


def message_time_us(message: dict) -> int:
    """When the exchange produced a `live_orders` message, in microseconds."""
    return int(message["data"]["microtimestamp"])


@dataclass(frozen=True)
class Snapshot:
    """The book as the exchange published it at `time_us`. Orders are
    `(order_id, side, price, qty)`, in the order of the answer."""

    time_us: int
    orders: Tuple[Tuple[int, Side, int, int], ...]

    @classmethod
    def parse(cls, body: dict, instrument: Instrument) -> "Snapshot":
        orders = []
        for key, side in (("bids", Side.BUY), ("asks", Side.SELL)):
            for price, amount, order_id in body[key]:
                orders.append((int(order_id), side, to_int(price, instrument.price_decimals),
                               to_int(amount, instrument.qty_decimals)))
        return cls(int(body["microtimestamp"]), tuple(orders))

    @property
    def first_message_us(self) -> int:
        """The time of the first messages to apply on top of the snapshot:
        those of its own millisecond, since message times are cut to it."""
        return self.time_us - self.time_us % 1000

    def as_orders(self) -> Dict[int, Tuple[Side, int, int]]:
        return {order_id: (side, price, qty) for order_id, side, price, qty in self.orders if qty > 0}


class Normaliser:
    """Holds the book the messages describe and emits the events that rebuild
    it. The events it returns carry no sequence number: the writer numbers
    them."""

    def __init__(self, instrument: Instrument):
        self.instrument = instrument
        self.book = Book()
        #: Messages that did not fit the book, by kind, plus `amended_order`
        #: (a resting order repriced or enlarged: legitimate, never observed).
        self.defects: Counter = Counter()
        self._takers: Dict[int, Tuple[Side, int, int]] = {}
        self._last_ts = 0
        #: Messages before this time may already be in the snapshot the book
        #: was loaded from (see `load`).
        self._settled_ts = 0

    # ----------------------------------------------------------- whole books

    def load(self, snapshot: Snapshot, ts_recv: int) -> List[Event]:
        """Replaces the book by a snapshot: a Clear, then one Add per order.

        The messages to apply next are those from `snapshot.first_message_us`
        on. The ones of the snapshot's own millisecond cannot be told apart
        from it (they may be in it or not), so they are applied leniently: a
        message that describes what the book already shows changes nothing
        and is not counted as a defect.
        """
        self.book.clear()
        self._takers = {}
        for order_id, (side, price, qty) in snapshot.as_orders().items():
            self.book.add(order_id, side, price, qty)
        self._last_ts = snapshot.time_us * 1000
        self._settled_ts = (snapshot.first_message_us + 1000) * 1000
        return self.dump(ts_recv)

    def dump(self, ts_recv: int) -> List[Event]:
        """The book as it stands, as a Clear and one Add per order: what a
        file starts with, so that it replays without the one before it."""
        ts = self._last_ts
        events = [Event(ts, ts_recv, 0, 0, 0, 0, EventType.CLEAR, Side.BUY)]
        events.extend(Event(ts, ts_recv, 0, order_id, price, qty, EventType.ADD, side)
                      for order_id, (side, price, qty) in self.book.orders.items())
        return events

    # -------------------------------------------------------------- messages

    def apply(self, message: dict, ts_recv: int) -> List[Event]:
        """The events of one `live_orders` message."""
        data = message["data"]
        name = message["event"]
        ts = int(data["microtimestamp"]) * 1000
        # A taker's fills follow its creation at once: the next order, or the
        # next millisecond, is another transaction.
        events = self.flush(ts_recv) if name == "order_created" or ts != self._last_ts else []
        self._last_ts = ts
        settling = ts < self._settled_ts
        book = self.book
        order_id = int(data["id"])
        left = to_int(data["amount_str"], self.instrument.qty_decimals)

        if name == "order_created":
            side = Side.BUY if data["order_type"] == 0 else Side.SELL
            price = to_int(data["price_str"], self.instrument.price_decimals)
            if order_id in book.orders or order_id in self._takers:
                if not settling:
                    self.defects["duplicate_order"] += 1
            elif left <= 0:
                self.defects["empty_order"] += 1
            elif self._marketable(side, price):
                self._takers[order_id] = (side, price, left)
            else:
                book.add(order_id, side, price, left)
                events.append(Event(ts, ts_recv, 0, order_id, price, left, EventType.ADD, side))
            return events

        deleted = name == "order_deleted"
        if order_id in self._takers:
            if deleted or left <= 0:
                del self._takers[order_id]
            else:
                side, price, _ = self._takers[order_id]
                self._takers[order_id] = (side, price, left)
            return events

        resting = book.orders.get(order_id)
        if resting is None:
            if not settling:
                self.defects["unknown_order"] += 1
            return events

        side, price, qty = resting
        if settling and not deleted and left == qty:
            return events  # the snapshot already showed the order after this message
        traded = min(to_int(data["amount_traded"], self.instrument.qty_decimals), qty)
        if traded > 0:
            # The side of a Trade is the aggressor's: the other one.
            events.append(Event(ts, ts_recv, 0, order_id, price, traded, EventType.TRADE, Side(1 - side)))
            qty = book.reduce(order_id, traded)
        target = 0 if deleted else left
        new_price = price if deleted else to_int(data["price_str"], self.instrument.price_decimals)
        if target > 0 and (new_price != price or target > qty):
            # Repriced or enlarged: it loses its place in the queue.
            if qty > 0:
                events.append(Event(ts, ts_recv, 0, order_id, price, qty, EventType.CANCEL, side))
                book.reduce(order_id, qty)
            book.add(order_id, side, new_price, target)
            events.append(Event(ts, ts_recv, 0, order_id, new_price, target, EventType.ADD, side))
            self.defects["amended_order"] += 1
        elif target < qty:
            events.append(Event(ts, ts_recv, 0, order_id, price, qty - target, EventType.CANCEL, side))
            book.reduce(order_id, qty - target)
        return events

    def flush(self, ts_recv: int) -> List[Event]:
        """Ends the transaction in progress: what is left of its takers rests."""
        events = []
        if self._takers:
            for order_id, (side, price, qty) in self._takers.items():
                self.book.add(order_id, side, price, qty)
                events.append(Event(self._last_ts, ts_recv, 0, order_id, price, qty, EventType.ADD, side))
            self._takers = {}
        if self.book.crossed():
            self.defects["crossed_book"] += 1
        return events

    def _marketable(self, side: Side, price: int) -> bool:
        if side is Side.BUY:
            ask = self.book.best(Side.SELL)
            return ask is not None and price >= ask
        bid = self.book.best(Side.BUY)
        return bid is not None and price <= bid
