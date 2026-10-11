"""The order-by-order book a stream of events describes.

Kept by the capture to know what it is recording (is this order resting? does
this one cross?) and by the verification to replay a file. It is as strict as
the book of `quant-microstructure` that will read the files: an Add of an id
already resting or a reduction of more than what rests is an error there, so
it is one here.
"""

from __future__ import annotations

import heapq
from typing import Dict, List, Optional, Tuple

from .events import Event, EventType, Side


class _Prices:
    """The prices of one side that hold at least one order, and the best."""

    def __init__(self, side: Side):
        # A min-heap: bids are stored negated so that the best comes first.
        self._sign = -1 if side is Side.BUY else 1
        self._count: Dict[int, int] = {}
        self._heap: List[int] = []

    def add(self, price: int) -> None:
        count = self._count.get(price, 0)
        if count == 0:
            heapq.heappush(self._heap, self._sign * price)
        self._count[price] = count + 1

    def remove(self, price: int) -> None:
        count = self._count[price] - 1
        if count:
            self._count[price] = count
            return
        del self._count[price]
        # Its heap entry stays until it surfaces; rebuild when those pile up.
        if len(self._heap) > 4 * len(self._count) + 1024:
            self._heap = [self._sign * p for p in self._count]
            heapq.heapify(self._heap)

    def best(self) -> Optional[int]:
        heap = self._heap
        while heap and self._sign * heap[0] not in self._count:
            heapq.heappop(heap)
        return self._sign * heap[0] if heap else None


class Book:
    """Resting orders: `orders` maps an id to `(side, price, qty)`. A dict
    keeps insertion order, which is the time priority within a price."""

    def __init__(self) -> None:
        self.orders: Dict[int, Tuple[Side, int, int]] = {}
        self._prices = {Side.BUY: _Prices(Side.BUY), Side.SELL: _Prices(Side.SELL)}

    def add(self, order_id: int, side: Side, price: int, qty: int) -> None:
        if qty <= 0 or order_id in self.orders:
            raise ValueError(f"add: order {order_id} is already resting or has no quantity")
        self.orders[order_id] = (side, price, qty)
        self._prices[side].add(price)

    def reduce(self, order_id: int, qty: int) -> int:
        """Takes `qty` off an order, which keeps its place, and removes it
        when nothing is left. Returns what is left."""
        resting = self.orders.get(order_id)
        if resting is None or not 0 < qty <= resting[2]:
            raise ValueError(f"reduce: order {order_id} is not resting or has less than {qty}")
        side, price, left = resting[0], resting[1], resting[2] - qty
        if left:
            self.orders[order_id] = (side, price, left)
        else:
            del self.orders[order_id]
            self._prices[side].remove(price)
        return left

    def clear(self) -> None:
        self.orders = {}
        self._prices = {Side.BUY: _Prices(Side.BUY), Side.SELL: _Prices(Side.SELL)}

    def apply(self, event: Event) -> None:
        if event.type is EventType.ADD:
            self.add(event.order_id, event.side, event.price, event.qty)
        elif event.type in (EventType.CANCEL, EventType.TRADE):
            self.reduce(event.order_id, event.qty)
        elif event.type is EventType.CLEAR:
            self.clear()
        else:
            raise ValueError("apply: not an event of an order-by-order stream")

    def best(self, side: Side) -> Optional[int]:
        return self._prices[side].best()

    def crossed(self) -> bool:
        bid, ask = self.best(Side.BUY), self.best(Side.SELL)
        return bid is not None and ask is not None and bid >= ask
