from __future__ import annotations

import math

from typing import Optional, override

from application.marketdata.store.tradeledger import TradeLedger
from common.type.numpycircularbuffer import NumpyCircularBuffer
from common.utils.math import mean, median, weighted_mean
from core.node.enginetimeprovider import EngineTimeProvider
from generated.type.common.enum.symbol import SymbolEnum
from generated.type.common.enum.venue import VenueEnum
from generated.type.marketdata.msg.trade import TradeMsg


class TradePriceSignalsTradeLedger(TradeLedger):
    
    class _Returns:

        __slots__ = (
            '_quantities', 
            '_simple_returns', 
            '_log_returns', 
            '_return_times', 
            '_return_engine_times'
        )

        def __init__(self, max_returns: int):
            self._quantities = NumpyCircularBuffer[float](max_length=max_returns, data_type=float)
            self._simple_returns = NumpyCircularBuffer[float](max_length=max_returns, data_type=float)
            self._log_returns = NumpyCircularBuffer[float](max_length=max_returns, data_type=float)
            self._return_times = NumpyCircularBuffer[int](max_length=max_returns, data_type=int)
            self._return_engine_times = NumpyCircularBuffer[int](max_length=max_returns, data_type=int)

        
        def append(self, msg: TradeMsg, previous_trade_price: float) -> None:
            self._quantities.append(msg.quantity)
            self._simple_returns.append(msg.price - previous_trade_price)
            self._log_returns.append(math.log(msg.price) - math.log(previous_trade_price))
            self._return_times.append(msg.time)
            self._return_engine_times.append(msg.context.engine_time)
        
        @property
        def quantities(self) -> NumpyCircularBuffer[float]:
            return self._quantities
        
        @property
        def simple(self) -> NumpyCircularBuffer[float]:
            return self._simple_returns
        
        @property
        def log(self) -> NumpyCircularBuffer[float]:
            return self._log_returns
        
        @property
        def times(self) -> NumpyCircularBuffer[int]:
            return self._return_times
        
        @property
        def engine_times(self) -> NumpyCircularBuffer[int]:
            return self._return_engine_times
        
        def clear(self) -> None:
            self._quantities.clear()
            self._simple_returns.clear()
            self._log_returns.clear()
            self._return_times.clear()
            self._return_engine_times.clear()
    
    MAX_RETURNS = 1_000_000

    __slots__ = ('_return_obj', '_returns')

    def __init__(self, venue: VenueEnum, symbol: SymbolEnum, engine_time_provider: EngineTimeProvider):
        super().__init__(venue=venue, symbol=symbol, engine_time_provider=engine_time_provider)
        self._returns = TradePriceSignalsTradeLedger._Returns(max_returns=self.MAX_RETURNS)
    
    @override
    def on_trade(self, msg: TradeMsg) -> None:
        super().on_trade(msg)
        if super().size > 1:
            self._returns.append(msg=msg, previous_trade_price=self._trades.prices[-2])
    
    def mean_simple_return(
        self, 
        window_returns: Optional[int] = None, 
        window_nanos: Optional[int] = None,
    ) -> Optional[float]:
        start_index = self._resolve_window_start_index(
            window_count=window_returns, 
            window_nanos=window_nanos, 
            times=self._returns.engine_times,
        )
        if start_index is None:
            return None
        return mean(values=self._returns.simple[start_index:])
    
    def weighted_mean_simple_return(
        self, 
        window_returns: Optional[int] = None, 
        window_nanos: Optional[int] = None,
    ) -> Optional[float]:
        start_index = self._resolve_window_start_index(
            window_count=window_returns, 
            window_nanos=window_nanos, 
            times=self._returns.engine_times,
        )
        if start_index is None:
            return None
        return weighted_mean(values=self._returns.simple[start_index:], weights=self._returns.quantities[start_index:])
    
    def median_simple_return(
        self, 
        window_returns: Optional[int] = None, 
        window_nanos: Optional[int] = None,
    ) -> Optional[float]:
        start_index = self._resolve_window_start_index(
            window_count=window_returns, 
            window_nanos=window_nanos, 
            times=self._returns.engine_times,
        )
        if start_index is None:
            return None
        return median(values=self._returns.simple[start_index:])
    
    def mean_log_return(
        self, 
        window_returns: Optional[int] = None, 
        window_nanos: Optional[int] = None,
    ) -> Optional[float]:
        start_index = self._resolve_window_start_index(
            window_count=window_returns, 
            window_nanos=window_nanos, 
            times=self._returns.engine_times,
        )
        if start_index is None:
            return None
        return mean(values=self._returns.log[start_index:])
    
    def weighted_mean_log_return(
        self, 
        window_returns: Optional[int] = None, 
        window_nanos: Optional[int] = None,
    ) -> Optional[float]:
        start_index = self._resolve_window_start_index(
            window_count=window_returns, 
            window_nanos=window_nanos, 
            times=self._returns.engine_times,
        )
        if start_index is None:
            return None
        return weighted_mean(values=self._returns.log[start_index:], weights=self._returns.quantities[start_index:])
    
    def median_log_return(
        self, 
        window_returns: Optional[int] = None, 
        window_nanos: Optional[int] = None,
    ) -> Optional[float]:
        start_index = self._resolve_window_start_index(
            window_count=window_returns, 
            window_nanos=window_nanos, 
            times=self._returns.engine_times,
        )
        if start_index is None:
            return None
        return median(values=self._returns.log[start_index:])