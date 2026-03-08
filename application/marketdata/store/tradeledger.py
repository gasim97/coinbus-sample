from __future__ import annotations

from bisect import bisect_right
from dataclasses import dataclass
from typing import Iterator, Optional, Self, Sequence

from common.type.numpycircularbuffer import NumpyCircularBuffer
from common.utils.math import (
    exponential_moving_average, 
    exponential_moving_average_vector, 
    max_value, 
    mean, 
    median, 
    mid_value, 
    min_value, 
    weighted_mean,
)
from common.utils.memo import LastValueMemoItem, Memo, with_memo
from core.node.enginetimeprovider import EngineTimeProvider
from generated.type.core.constants import INVALID_PRICE_FLOAT, INVALID_QUANTITY_FLOAT, INVALID_TIMESTAMP
from generated.type.common.enum.symbol import SymbolEnum
from generated.type.common.enum.venue import VenueEnum
from generated.type.marketdata.msg.trade import TradeMsg


class TradeLedger:

    @dataclass
    class Trade:
        price: float = INVALID_PRICE_FLOAT
        quantity: float = INVALID_QUANTITY_FLOAT
        time: int = INVALID_TIMESTAMP
        engine_time: int = INVALID_TIMESTAMP

        @staticmethod
        def copy_from_trade_msg(trade: TradeLedger.Trade, trade_msg: TradeMsg) -> TradeLedger.Trade:
            trade.price = trade_msg.price
            trade.quantity = trade_msg.quantity
            trade.time = trade_msg.time
            trade.engine_time = trade_msg.context.engine_time
            return trade

        def clear(self) -> Self:
            self.price = INVALID_PRICE_FLOAT
            self.quantity = INVALID_QUANTITY_FLOAT
            self.time = INVALID_TIMESTAMP
            self.engine_time = INVALID_TIMESTAMP
            return self
    
    class _Trades:

        __slots__ = (
            '_trade_prices', 
            '_trade_quantities', 
            '_trade_times', 
            '_trade_engine_times', 
        )

        def __init__(self, max_trades: int):
            self._trade_prices = NumpyCircularBuffer[float](max_length=max_trades, data_type=float)
            self._trade_quantities = NumpyCircularBuffer[float](max_length=max_trades, data_type=float)
            self._trade_times = NumpyCircularBuffer[int](max_length=max_trades, data_type=int)
            self._trade_engine_times = NumpyCircularBuffer[int](max_length=max_trades, data_type=int)

        def append(self, msg: TradeMsg) -> None:
            self._trade_prices.append(msg.price)
            self._trade_quantities.append(msg.quantity)
            self._trade_times.append(msg.time)
            self._trade_engine_times.append(msg.context.engine_time)
        
        @property
        def size(self) -> int:
            return len(self._trade_prices)

        @property
        def prices(self) -> Sequence[float]:
            return self._trade_prices

        @property
        def quantities(self) -> Sequence[float]:
            return self._trade_quantities

        @property
        def times(self) -> Sequence[int]:
            return self._trade_times

        @property
        def engine_times(self) -> Sequence[int]:
            return self._trade_engine_times
        
        def clear(self) -> None:
            self._trade_prices.clear()
            self._trade_quantities.clear()
            self._trade_times.clear()
            self._trade_engine_times.clear()

    MAX_TRADES = 1_000_000

    __slots__ = (
        '_venue', 
        '_symbol', 
        '_engine_time_provider', 
        '_trade_obj', 
        '_trades', 
        '_parameter_memo_keys'
    )

    def __init__(
        self, 
        venue: VenueEnum, 
        symbol: SymbolEnum, 
        engine_time_provider: EngineTimeProvider,
    ):
        self._venue = venue
        self._symbol = symbol
        self._engine_time_provider = engine_time_provider
        self._trade_obj = TradeLedger.Trade()
        self._trades = TradeLedger._Trades(max_trades=self.MAX_TRADES)
        self._parameter_memo_keys: dict[int, dict[str, str]] = {}

    def on_trade(self, msg: TradeMsg) -> None:
        self._trades.append(msg)

    @property
    def venue(self) -> VenueEnum:
        return self._venue

    @property
    def symbol(self) -> SymbolEnum:
        return self._symbol

    @property
    def size(self) -> int:
        return self._trades.size

    @property
    def latest(self) -> Optional[TradeLedger.Trade]:
        return self.previous(index=0)

    @property
    def latest_time(self) -> int:
        latest = self.latest
        if latest is None:
            return INVALID_TIMESTAMP
        return latest.time

    @property
    def is_latest_new_price(self) -> bool:
        if self.size < 2:
            return self.size == 1
        return self._trades.prices[-1] != self._trades.prices[-2]

    def previous(self, index: int) -> Optional[TradeLedger.Trade]:
        if self.size <= index:
            return None
        return self._wrap_trade(-(index + 1))
    
    def _wrap_trade(self, index: int) -> TradeLedger.Trade:
        self._trade_obj.time = self._trades.times[index]
        self._trade_obj.engine_time = self._trades.engine_times[index]
        self._trade_obj.price = self._trades.prices[index]
        self._trade_obj.quantity = self._trades.quantities[index]
        return self._trade_obj

    def __iter__(self) -> Iterator[TradeLedger.Trade]:
        for i in range(self.size):
            yield self._wrap_trade(i)
    
    def _resolve_window_start_index(
        self, 
        window_count: Optional[int] = None, 
        window_nanos: Optional[int] = None,
        times: Optional[Sequence[int]] = None,
    ) -> Optional[int]:
        if window_count is not None:
            return self._resolve_count_based_window_start_index(window_count=window_count)
        if window_nanos is not None and times is not None:
            return self._resolve_time_based_window_start_index(window_nanos=window_nanos, times=times)
        return None

    def _resolve_count_based_window_start_index(self, window_count: int) -> Optional[int]:
        if self.size == 0:
            return None
        return -window_count if window_count > 0 else None
    
    def _resolve_time_based_window_start_index(self, window_nanos: int, times: Sequence[int]) -> Optional[int]:
        if len(times) == 0:
            return None
        start_time = self._engine_time_provider.engine_time - window_nanos
        if not times[0] <= start_time < times[-1]:
            return None
        start_index = bisect_right(times, start_time)
        return start_index if start_index != len(times) else None

    def max_price(
        self, 
        window_trades: Optional[int] = None, 
        window_nanos: Optional[int] = None,
    ) -> Optional[float]:
        start_index = self._resolve_window_start_index(
            window_count=window_trades, 
            window_nanos=window_nanos, 
            times=self._trades.engine_times,
        )
        if start_index is None:
            return None
        return max_value(values=self._trades.prices[start_index:])

    def min_price(
        self, 
        window_trades: Optional[int] = None, 
        window_nanos: Optional[int] = None,
    ) -> Optional[float]:
        start_index = self._resolve_window_start_index(
            window_count=window_trades, 
            window_nanos=window_nanos, 
            times=self._trades.engine_times,
        )
        if start_index is None:
            return None
        return min_value(values=self._trades.prices[start_index:])

    def mid_price(
        self, 
        window_trades: Optional[int] = None, 
        window_nanos: Optional[int] = None,
    ) -> Optional[float]:
        start_index = self._resolve_window_start_index(
            window_count=window_trades, 
            window_nanos=window_nanos, 
            times=self._trades.engine_times,
        )
        if start_index is None:
            return None
        return mid_value(values=self._trades.prices[start_index:])

    def mean_price(
        self, 
        window_trades: Optional[int] = None, 
        window_nanos: Optional[int] = None,
    ) -> Optional[float]:
        start_index = self._resolve_window_start_index(
            window_count=window_trades, 
            window_nanos=window_nanos, 
            times=self._trades.engine_times,
        )
        if start_index is None:
            return None
        return mean(values=self._trades.prices[start_index:])

    def weighted_mean_price(
        self, 
        window_trades: Optional[int] = None, 
        window_nanos: Optional[int] = None,
    ) -> Optional[float]:
        start_index = self._resolve_window_start_index(
            window_count=window_trades, 
            window_nanos=window_nanos, 
            times=self._trades.engine_times,
        )
        if start_index is None:
            return None
        return weighted_mean(values=self._trades.prices[start_index:], weights=self._trades.quantities[start_index:])

    def median_price(
        self, 
        window_trades: Optional[int] = None, 
        window_nanos: Optional[int] = None,
    ) -> Optional[float]:
        start_index = self._resolve_window_start_index(
            window_count=window_trades, 
            window_nanos=window_nanos, 
            times=self._trades.engine_times,
        )
        if start_index is None:
            return None
        return median(values=self._trades.prices[start_index:])
    
    @with_memo(key_type=tuple[VenueEnum, SymbolEnum, float], item_type=LastValueMemoItem[float])
    def exponential_moving_average_price(
        self, decay_factor: float, memo: Memo[tuple[VenueEnum, SymbolEnum, float], LastValueMemoItem[float]],
    ) -> Optional[float]:
        last_value_memo = memo.get_or_create(
            key=(self._venue, self._symbol, decay_factor), 
            value=None, 
            time=None,
        )
        start_index = (
            self._resolve_time_based_window_start_index(
                window_nanos=self._engine_time_provider.engine_time - last_value_memo.time,
                times=self._trades.engine_times,
            )
            if last_value_memo.time is not None
            else 0
        )
        if start_index is not None:
            last_value_memo.update(
                value=exponential_moving_average(
                    values=self._trades.prices[start_index:], decay_factor=decay_factor, last_value=last_value_memo.value
                ),
                time=self._engine_time_provider.engine_time,
            )
        return last_value_memo.value
    
    @with_memo(key_type=tuple[VenueEnum, SymbolEnum, float, str], item_type=LastValueMemoItem[float])
    def exponential_moving_average_price_vector(
        self, 
        decay_factor: float, 
        signal_id: str,
        memo: Memo[tuple[VenueEnum, SymbolEnum, float, str], LastValueMemoItem[float]],
    ) -> Optional[list[float]]:
        last_value_memo = memo.get_or_create(
            key=(self._venue, self._symbol, decay_factor, signal_id), 
            value=None, 
            time=None,
        )
        start_index = (
            self._resolve_time_based_window_start_index(
                window_nanos=self._engine_time_provider.engine_time - last_value_memo.time,
                times=self._trades.engine_times,
            )
            if last_value_memo.time is not None
            else 0
        )
        if start_index is None:
            return None
        result = exponential_moving_average_vector(
            values=self._trades.prices[start_index:], decay_factor=decay_factor, last_value=last_value_memo.value
        )
        last_value_memo.update(value=result[-1], time=self._engine_time_provider.engine_time)
        return result