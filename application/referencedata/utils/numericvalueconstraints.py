from typing import Optional, Self

from common.type.wfloat import WFloat
from generated.type.common.enum.side import SideEnum
from generated.type.referencedata.msg.numericvaluebounds import NumericValueBoundsMsg
from generated.type.referencedata.msg.numericvalueinfo import NumericValueInfoMsg
from generated.type.referencedata.msg.symbolinfo import SymbolInfoMsg


class NumericValueBounds:

    __slots__ = ('_min', '_max')

    def set(
        self, 
        info: NumericValueBoundsMsg, 
        min: Optional[float] = None, 
        max: Optional[float] = None, 
        multiplier: float = 1,
    ) -> Self:
        self._min = (min or info.min or 0.0) * multiplier
        self._max = (max or info.max or 0.0) * multiplier
        return self
    
    @property
    def min(self) -> float:
        return self._min
    
    @property
    def max(self) -> float:
        return self._max
    
    def clear(self) -> Self:
        self._min = 0.0
        self._max = 0.0
        return self


class NumericValueInfo:

    __slots__ = ('_precision', '_step_size', '_min', '_max')

    def set(
        self, 
        info: NumericValueInfoMsg, 
        precision: Optional[int] = None, 
        step_size: Optional[float] = None, 
        min: Optional[float] = None, 
        max: Optional[float] = None,
        multiplier: float = 1,
    ) -> Self:
        self._precision = precision or info.precision or 0
        self._step_size = WFloat(value=step_size or info.step_size or 0.0, precision=self._precision) * multiplier
        self._min = WFloat(value=min or info.min or 0.0, precision=self._precision) * multiplier
        self._max = WFloat(value=max or info.max or 0.0, precision=self._precision) * multiplier
        return self
    
    @property
    def precision(self) -> int:
        return self._precision
    
    @property
    def step_size(self) -> WFloat:
        return self._step_size
    
    @property
    def min(self) -> WFloat:
        return self._min
    
    @property
    def max(self) -> WFloat:
        return self._max
    
    def clear(self) -> Self:
        self._precision = 0
        self._step_size = WFloat(value=0.0, precision=0)
        self._min = WFloat(value=0.0, precision=0)
        self._max = WFloat(value=0.0, precision=0)
        return self


class NotionalConstraints(NumericValueBounds):
    
    def apply(self, quantity: float, price: float) -> bool:
        notional = quantity * price
        return self._min <= notional <= self._max

    def init_base_asset(self, symbol_info: SymbolInfoMsg) -> Self:
        if symbol_info.notional is None:
            return self.clear()
        return self.set(info=symbol_info.notional)

    def init_quote_asset(self, symbol_info: SymbolInfoMsg, price: float) -> Self:
        if symbol_info.notional is None:
            return self.clear()
        return self.set(info=symbol_info.notional, multiplier=price)


class PriceConstraints(NumericValueInfo):
    
    def apply(self, price: float, side: SideEnum) -> Optional[WFloat]:
        if side == SideEnum.BUY:
            constrained_price = WFloat.round_down(value=price, precision=self._precision)
        else:
            constrained_price = WFloat.round_up(value=price, precision=self._precision)
        constrained_price -= constrained_price % self._step_size
        if not self._min <= constrained_price.as_float <= self._max:
            return None
        return constrained_price

    def init_price(self, symbol_info: SymbolInfoMsg) -> Self:
        if symbol_info.price is None:
            return self.clear()
        return self.set(info=symbol_info.price)


class QuantityConstraints(NumericValueInfo):
    
    def apply(self, quantity: float) -> Optional[WFloat]:
        constrained_quantity = WFloat.round_down(value=quantity, precision=self._precision)
        if self._step_size > 0:
            constrained_quantity -= constrained_quantity % self._step_size
        if not self._min <= constrained_quantity.as_float <= self._max:
            return None
        return constrained_quantity
    
    def init_base_asset(self, symbol_info: SymbolInfoMsg) -> Self:
        if symbol_info.quantity is None:
            return self.clear()
        return self.set(info=symbol_info.quantity)

    def init_quote_asset(self, symbol_info: SymbolInfoMsg, price: float) -> Self:
        if symbol_info.quantity is None:
            return self.clear()
        return self.set(
            info=symbol_info.quantity, precision=symbol_info.quote_asset_precision, multiplier=price,
        )

    def init_market_order_quote_asset(self, symbol_info: SymbolInfoMsg) -> Self:
        if symbol_info.market_quantity is None:
            return self.clear()
        return self.set(info=symbol_info.market_quantity)