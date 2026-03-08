from __future__ import annotations

from dataclasses import dataclass
from pandas import DataFrame
from sortedcontainers import SortedDict
from typing import Optional, Sequence

from common.utils.math import weighted_mean
from common.utils.pool import ObjectPool, ObjectPoolManager
from generated.type.common.enum.side import SideEnum
from generated.type.common.enum.symbol import SymbolEnum
from generated.type.common.enum.venue import VenueEnum
from generated.type.core.constants import INVALID_PRICE_FLOAT, INVALID_QUANTITY_FLOAT, INVALID_TIMESTAMP
from generated.type.marketdata.enum.bookside import BookSideEnum
from generated.type.marketdata.msg.depthupdate import DepthUpdateMsg


class OrderBook:

    @dataclass(slots=True)
    class BookLevel:
        price: float = 0.0
        volume: float = 0.0

    INVALID_BOOK_LEVEL = BookLevel(price=INVALID_PRICE_FLOAT, volume=INVALID_QUANTITY_FLOAT)
    _BOOK_LEVEL_POOL_SIZE = 10_000

    __slots__ = ('_venue', '_symbol', '_bids', '_asks', '_last_update_time_nanos', '_book_level_pool')

    def __init__(self, venue: VenueEnum, symbol: SymbolEnum, pool_manager: ObjectPoolManager):
        self._venue = venue
        self._symbol = symbol
        self._book_level_pool: ObjectPool[OrderBook.BookLevel] = pool_manager.pre_sized_pool(
            type=OrderBook.BookLevel, 
            expected_size=self._BOOK_LEVEL_POOL_SIZE,
        )
        self._bids: SortedDict[float, OrderBook.BookLevel] = SortedDict(lambda x: -x)
        self._asks: SortedDict[float, OrderBook.BookLevel] = SortedDict()
        self._last_update_time_nanos = INVALID_TIMESTAMP
    
    def __str__(self) -> str:
        return f"{self._symbol} Order Book\n{self.order_book_levels_string() or 'Empty'}"
    
    def clear(self) -> None:
        for book_level in self._bids.values():
            self._book_level_pool.release(object=book_level)
        self._bids.clear()
        for book_level in self._asks.values():
            self._book_level_pool.release(object=book_level)
        self._asks.clear()
    
    def _create_book_level(self, price: float, volume: float) -> OrderBook.BookLevel:
        book_level = self._book_level_pool.get()
        book_level.price = price
        book_level.volume = volume
        return book_level

    @property
    def venue(self) -> VenueEnum:
        return self._venue

    @property
    def symbol(self) -> SymbolEnum:
        return self._symbol
    
    @property
    def bids(self) -> Sequence[OrderBook.BookLevel]:
        return self._bids.values()
    
    @property
    def asks(self) -> Sequence[OrderBook.BookLevel]:
        return self._asks.values()
    
    @property
    def best_bid(self) -> OrderBook.BookLevel:
        return self.level(side=BookSideEnum.BIDS, level=0)
    
    @property
    def best_ask(self) -> OrderBook.BookLevel:
        return self.level(side=BookSideEnum.ASKS, level=0)
    
    @property
    def bid_depth(self) -> int:
        return len(self._bids)
    
    @property
    def ask_depth(self) -> int:
        return len(self._asks)
    
    @property
    def mid_price(self) -> float:
        best_bid = self.best_bid
        best_ask = self.best_ask
        if best_bid.price == 0 or best_ask.price == 0:
            return INVALID_PRICE_FLOAT
        return (best_bid.price + best_ask.price) / 2
    
    def near_touch(self, order_side: SideEnum) -> float:
        return self.best_ask.price if order_side == SideEnum.SELL else self.best_bid.price
    
    def far_touch(self, order_side: SideEnum) -> float:
        return self.best_bid.price if order_side == SideEnum.SELL else self.best_ask.price
    
    def _side(self, side: BookSideEnum) -> SortedDict:
        return self._bids if side == BookSideEnum.BIDS else self._asks
    
    def level(self, side: BookSideEnum, level: int = 0) -> OrderBook.BookLevel:
        order_book_side = self._side(side=side)
        try:
            return order_book_side.peekitem(index=level)[1]
        except IndexError:
            return self.INVALID_BOOK_LEVEL
    
    def side_vwap(self, side: BookSideEnum, depth: Optional[int] = None) -> float:
        order_book_side = self._side(side=side)
        if depth is not None and len(order_book_side) < depth:
            return INVALID_PRICE_FLOAT
        order_book_side_levels = order_book_side.values()[:depth]
        return weighted_mean(
            values=[x.price for x in order_book_side_levels], 
            weights=[x.volume for x in order_book_side_levels],
        )
    
    def vwap(self, depth: Optional[int] = None) -> float:
        if depth is not None and (len(self._bids) < depth or len(self._asks) < depth):
            return INVALID_PRICE_FLOAT
        combined_order_book_levels = self._bids.values()[:depth] + self._asks.values()[:depth]
        return weighted_mean(
            values=[x.price for x in combined_order_book_levels], 
            weights=[x.volume for x in combined_order_book_levels],
        )
    
    def side_volume(self, side: BookSideEnum, depth: Optional[int] = None) -> float:
        order_book_side = self._side(side=side)
        if depth is not None and len(order_book_side) < depth:
            return INVALID_QUANTITY_FLOAT
        return sum(x.volume for x in order_book_side.values()[:depth])
    
    def volume(self, depth: Optional[int] = None) -> float:
        if depth is not None and (len(self._bids) < depth or len(self._asks) < depth):
            return INVALID_QUANTITY_FLOAT
        combined_order_book_levels = self._bids.values()[:depth] + self._asks.values()[:depth]
        return sum(x.volume for x in combined_order_book_levels)
    
    def order_book_levels_string(self, depth: int = 10) -> Optional[str]:
        asks = self._asks.values()
        bids = self._bids.values()
        if len(asks) < depth or len(bids) < depth:
            return None
        df = DataFrame(data=[
            {"Bid Vol": bids[i].volume, "Bid Px": bids[i].price, "Ask Px": asks[i].price, "Ask Vol": asks[i].volume}
            for i in range(depth)
        ])
        return df.to_string()


class StreamOrderBook(OrderBook):

    __slots__ = ('_updates')

    ACTIVE_UPDATES_THRESHOLD = 120

    def __init__(self, venue: VenueEnum, symbol: SymbolEnum, pool_manager: ObjectPoolManager):
        super().__init__(venue=venue, symbol=symbol, pool_manager=pool_manager)
        self._updates = 0

    @property
    def active(self) -> bool:
        return self._updates >= self.ACTIVE_UPDATES_THRESHOLD 
    
    @property
    def last_update_time_nanos(self) -> int:
        return self._last_update_time_nanos

    def on_depth_update(self, msg: DepthUpdateMsg) -> None:
        if msg.symbol != self._symbol:
            return
        for price, volume in zip(msg.bid_prices, msg.bid_volumes):
            if volume == 0:
                book_level = self._bids.pop(price, None)
                if book_level is not None:
                    self._book_level_pool.release(object=book_level)
            else:
                book_level = self._bids.get(price)
                if book_level is not None:
                    book_level.volume = volume
                else:
                    self._bids[price] = self._create_book_level(price=price, volume=volume)
        for price, volume in zip(msg.ask_prices, msg.ask_volumes):
            if volume == 0:
                book_level = self._asks.pop(price, None)
                if book_level is not None:
                    self._book_level_pool.release(object=book_level)
            else:
                book_level = self._asks.get(price)
                if book_level is not None:
                    book_level.volume = volume
                else:
                    self._asks[price] = self._create_book_level(price=price, volume=volume)
        if msg.is_final_chunk:
            self._updates += 1
        self._last_update_time_nanos = msg.context.engine_time