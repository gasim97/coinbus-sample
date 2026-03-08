from __future__ import annotations

from typing import Optional, Self, override

from application.orderentry.order.order import Order
from common.type.wfloat import WFloat
from common.utils import symbolhelper
from generated.type.common.enum.side import SideEnum
from generated.type.common.enum.symbol import SymbolEnum
from generated.type.common.enum.ticker import TickerEnum
from generated.type.common.enum.venue import VenueEnum
from generated.type.core.constants import INVALID_TIMESTAMP
from generated.type.orderentry.constants import INVALID_EXTERNAL_ORDER_ID, INVALID_INTERNAL_ORDER_ID
from generated.type.orderentry.enum.ordertype import OrderTypeEnum
from generated.type.orderentry.enum.timeinforce import TimeInForceEnum
from generated.type.orderentry.msg.cancelorder import CancelOrderMsg
from generated.type.orderentry.msg.enterorder import EnterOrderMsg
from generated.type.orderentry.msg.fill import FillMsg
from generated.type.orderentry.msg.ordercancelled import OrderCancelledMsg
from generated.type.orderentry.msg.ordercompleted import OrderCompletedMsg
from generated.type.orderentry.msg.orderentered import OrderEnteredMsg
from generated.type.orderentry.msg.rejectcancelorder import RejectCancelOrderMsg
from generated.type.orderentry.msg.rejectenterorder import RejectEnterOrderMsg


class InternalOrder(Order):

    __slots__ = (
        '_internal_order_id',
        '_external_order_id',
        '_venue',
        '_symbol',
        '_side',
        '_quantity',
        '_quantity_asset',
        '_price',
        '_stop_price',
        '_reference_price',
        '_order_type',
        '_time_in_force',
        '_executed_quantity',
        '_executed_quote_quantity',
        '_is_rejected',
        '_is_cancelled',
        '_is_complete',
        '_create_time_nanos',
        '_last_update_time_nanos',
    )

    def __init__(self):
        super().__init__()
        self.clear()
    
    @override
    def clear(self) -> Self:
        self._internal_order_id = INVALID_INTERNAL_ORDER_ID
        self._external_order_id = INVALID_EXTERNAL_ORDER_ID
        self._venue = VenueEnum.INVALID
        self._symbol = SymbolEnum.INVALID
        self._side = SideEnum.INVALID
        self._quantity = WFloat(0, 0)
        self._quantity_asset = TickerEnum.INVALID
        self._price: Optional[WFloat] = None
        self._stop_price: Optional[WFloat] = None
        self._reference_price: Optional[WFloat] = None
        self._order_type = OrderTypeEnum.INVALID
        self._time_in_force = TimeInForceEnum.INVALID
        self._executed_quantity = WFloat(0, 0)
        self._executed_quote_quantity = WFloat(0, 0)
        self._is_rejected = False
        self._is_cancelled = False
        self._is_complete = False
        self._create_time_nanos: int = INVALID_TIMESTAMP
        self._last_update_time_nanos: int = INVALID_TIMESTAMP
        return self
    
    @property
    def internal_order_id(self) -> str:
        return self._internal_order_id
    
    @property
    def external_order_id(self) -> int:
        return self._external_order_id
    
    @property
    def venue(self) -> VenueEnum:
        return self._venue
    
    @property
    def symbol(self) -> SymbolEnum:
        return self._symbol
    
    @property
    def side(self) -> SideEnum:
        return self._side
    
    @property
    def is_buy(self) -> bool:
        return self._side == SideEnum.BUY
    
    @property
    def is_sell(self) -> bool:
        return self._side == SideEnum.SELL
    
    @property
    def quantity(self) -> WFloat:
        return self._quantity
    
    @property
    def quantity_asset(self) -> TickerEnum:
        return self._quantity_asset
    
    @property
    def is_quantity_base_asset(self) -> bool:
        return symbolhelper.is_base_asset(symbol=self.symbol, asset=self.quantity_asset)
    
    @property
    def is_quantity_quote_asset(self) -> bool:
        return symbolhelper.is_quote_asset(symbol=self.symbol, asset=self.quantity_asset)

    @property
    def leaves_quantity(self) -> WFloat:
        if self.is_quantity_base_asset:
            return self.quantity - self.executed_quantity
        return self.quantity - self.executed_quote_quantity
    
    @property
    def base_asset_quantity(self) -> WFloat:
        if self.is_quantity_base_asset:
            return self.quantity
        return self.quantity / (self.price_or_reference_price or 0)
    
    @property
    def quote_asset_quantity(self) -> WFloat:
        if self.is_quantity_quote_asset:
            return self.quantity
        return self.quantity * (self.price_or_reference_price or 0)
    
    @property
    def leaves_base_asset_quantity(self) -> WFloat:
        if self.is_quantity_base_asset:
            return self.leaves_quantity
        return self.leaves_quantity / (self.price_or_reference_price or 0)
    
    @property
    def leaves_quote_asset_quantity(self) -> WFloat:
        if self.is_quantity_quote_asset:
            return self.leaves_quantity
        return self.leaves_quantity * (self.price_or_reference_price or 0)
    
    @property
    def price(self) -> Optional[WFloat]:
        return self._price
    
    @property
    def stop_price(self) -> Optional[WFloat]:
        return self._stop_price
    
    @property
    def reference_price(self) -> Optional[WFloat]:
        return self._reference_price
    
    @property
    def price_or_reference_price(self) -> Optional[WFloat]:
        return self.price or self.reference_price

    @property
    def average_execution_price(self) -> float:
        if self.executed_quantity <= 0:
            return 0
        return self.executed_quote_quantity.as_float / self.executed_quantity.as_float
    
    @property
    def order_type(self) -> OrderTypeEnum:
        return self._order_type
    
    @property
    def time_in_force(self) -> TimeInForceEnum:
        return self._time_in_force
    
    @property
    def executed_quantity(self) -> WFloat:
        return self._executed_quantity
    
    @property
    def executed_quote_quantity(self) -> WFloat:
        return self._executed_quote_quantity
    
    @property
    def is_rejected(self) -> bool:
        return self._is_rejected
    
    @property
    def is_cancelled(self) -> bool:
        return self._is_cancelled
    
    @override
    @property
    def is_complete(self) -> bool:
        return self._is_complete or self._is_rejected or self._is_cancelled or self.leaves_quantity <= 0
    
    @property
    def is_working(self) -> bool:
        return not self.is_complete
    
    @property
    def create_time_nanos(self) -> int:
        return self._create_time_nanos
    
    @property
    def last_update_time_nanos(self) -> int:
        return self._last_update_time_nanos

    ### region event handlers

    @override
    def on_enter_order(self, msg: EnterOrderMsg) -> None:
        self._internal_order_id = msg.internal_order_id
        self._venue = msg.venue
        self._symbol = msg.symbol
        self._side = msg.side
        self._quantity = msg.quantity
        self._quantity_asset = msg.quantity_asset
        self._price = msg.price
        self._stop_price = msg.stop_price
        self._reference_price = msg.reference_price
        self._order_type = msg.order_type
        self._time_in_force = msg.time_in_force
        self._create_time_nanos = msg.context.engine_time
        self._last_update_time_nanos = msg.context.engine_time
        assert self.is_quantity_base_asset or self.is_quantity_quote_asset, \
            f"Invalid quantity asset {self.quantity_asset} for symbol {self.symbol}"

    @override
    def on_order_entered(self, msg: OrderEnteredMsg) -> None:
        self._external_order_id = msg.external_order_id
        self._last_update_time_nanos = msg.context.engine_time

    @override
    def on_reject_enter_order(self, msg: RejectEnterOrderMsg) -> None:
        self._is_rejected = True
        self._last_update_time_nanos = msg.context.engine_time

    @override
    def on_cancel_order(self, msg: CancelOrderMsg) -> None:
        self._last_update_time_nanos = msg.context.engine_time

    @override
    def on_order_cancelled(self, msg: OrderCancelledMsg) -> None:
        self._is_cancelled = True
        self._last_update_time_nanos = msg.context.engine_time

    @override
    def on_reject_cancel_order(self, msg: RejectCancelOrderMsg) -> None:
        self._last_update_time_nanos = msg.context.engine_time

    @override
    def on_fill(self, msg: FillMsg) -> None:
        self._executed_quantity += msg.quantity
        self._executed_quote_quantity += msg.quote_quantity
        self._last_update_time_nanos = msg.context.engine_time

    @override
    def on_order_completed(self, msg: OrderCompletedMsg) -> None:
        self._is_complete = True
        self._last_update_time_nanos = msg.context.engine_time

    ### end region