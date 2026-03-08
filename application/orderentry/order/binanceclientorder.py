from __future__ import annotations

from logging import Logger
from typing import Any, Callable, Optional, Self, override

from binance.client import Client  # type: ignore[import-untyped]
from binance.exceptions import BinanceAPIException  # type: ignore[import-untyped]

from application.orderentry.order.order import Order
from common.utils.time import nanos_to_millis, millis_to_nanos
from core.node.enginetimeprovider import EngineTimeProvider
from generated.type.common.enum.side import SideEnum
from generated.type.common.enum.symbol import SymbolEnum
from generated.type.common.enum.venue import VenueEnum
from generated.type.orderentry.constants import INVALID_EXTERNAL_ORDER_ID, INVALID_INTERNAL_ORDER_ID
from generated.type.orderentry.enum.orderstatus import OrderStatusEnum
from generated.type.orderentry.msg.cancelorder import CancelOrderMsg
from generated.type.orderentry.msg.enterorder import EnterOrderMsg
from generated.type.orderentry.msg.fill import FillMsg
from generated.type.orderentry.msg.ordercancelled import OrderCancelledMsg
from generated.type.orderentry.msg.ordercompleted import OrderCompletedMsg
from generated.type.orderentry.msg.orderentered import OrderEnteredMsg
from generated.type.orderentry.msg.rejectcancelorder import RejectCancelOrderMsg
from generated.type.orderentry.msg.rejectenterorder import RejectEnterOrderMsg


class BinanceClientOrder(Order):

    __slots__ = (
        '_logger',
        '_client_provider',
        '_engine_time_provider',
        '_order_request_validity_window_ms',
        '_internal_order_id',
        '_external_order_id',
        '_order',
        '_venue',
        '_symbol',
        '_create_time_nanos',
        '_last_update_time_nanos',
        '_fills',
        '_acked_fills',
    )

    def __init__(
        self, 
        logger: Logger, 
        client_provider: Callable[[], Client], 
        engine_time_provider: EngineTimeProvider,
        order_request_validity_window_ms: int,
    ):
        super().__init__()
        self._logger = logger
        self._client_provider = client_provider
        self._engine_time_provider = engine_time_provider
        self._order_request_validity_window_ms = order_request_validity_window_ms
        self.clear()

    @override
    def clear(self) -> Self:
        self._internal_order_id = INVALID_INTERNAL_ORDER_ID
        self._external_order_id = INVALID_EXTERNAL_ORDER_ID
        self._order: dict[str, Any] = {}
        self._venue = VenueEnum.INVALID
        self._symbol = SymbolEnum.INVALID
        self._create_time_nanos: Optional[int] = None
        self._last_update_time_nanos: Optional[int] = None
        self._fills: list[dict[str, Any]] = []
        self._acked_fills: dict[int, FillMsg] = {}
        return self

    def __str__(self) -> str:
        return self._order.__str__()

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
        return SideEnum[self._order["side"]]

    @property
    def symbol_str(self) -> str:
        return self._order["symbol"]

    @property
    def status(self) -> OrderStatusEnum:
        return OrderStatusEnum[self._order["status"]]

    @property
    def price(self) -> float:
        return float(self._order["price"])

    @property
    def price_str(self) -> str:
        return self._order["price"]

    @property
    def quantity(self) -> float:
        return float(self._order["origQty"])

    @property
    def quantity_str(self) -> str:
        return self._order["origQty"]

    @property
    def executed_quantity(self) -> float:
        return float(self._order["executedQty"])

    @property
    def executed_quantity_str(self) -> str:
        return self._order["executedQty"]

    @property
    def executed_quote_quantity(self) -> float:
        return float(self._order["cummulativeQuoteQty"])

    @property
    def executed_quote_quantity_str(self) -> str:
        return self._order["cummulativeQuoteQty"]

    @property
    def is_working(self) -> bool:
        return self._order["isWorking"]

    @override
    @property
    def is_complete(self) -> bool:
        return self.status in [
            OrderStatusEnum.FILLED,
            OrderStatusEnum.CANCELED,
            OrderStatusEnum.EXPIRED,
            OrderStatusEnum.EXPIRED_IN_MATCH,
        ]

    @property
    def is_cancelled(self) -> bool:
        return self.status == OrderStatusEnum.CANCELED

    @property
    def working_time_nanos(self) -> int:
        return millis_to_nanos(self._order["workingTime"])

    @property
    def create_time_nanos(self) -> Optional[int]:
        return self._create_time_nanos

    @property
    def last_update_time_nanos(self) -> Optional[int]:
        return self._last_update_time_nanos

    @property
    def acked_fills(self) -> dict[int, FillMsg]:
        return self._acked_fills

    @property
    def unacked_fills(self) -> list[dict[str, Any]]:
        return [fill for fill in self._fills if fill["id"] not in self._acked_fills]

    def _fetch(self, external_order_id: int, symbol: str) -> BinanceClientOrder:
        try:
            self._order = self._client_provider().get_order(
                orderId=external_order_id, 
                symbol=symbol, 
                recvWindow=self._order_request_validity_window_ms,
            )
            self._fills = [
                fill
                for fill in self._client_provider().get_my_trades(
                    symbol=symbol, 
                    startTime=nanos_to_millis(self.working_time_nanos, trim=True),
                    recvWindow=self._order_request_validity_window_ms,
                )
                if fill["orderId"] == external_order_id
            ]
        except BinanceAPIException:
            self._logger.error("Failed to fetch order due to BinanceAPIException", exc_info=True)
            return self._fetch(external_order_id=external_order_id, symbol=symbol)
        self._last_update_time_nanos = self._engine_time_provider.engine_time
        return self

    def fetch(self) -> BinanceClientOrder:
        return self._fetch(external_order_id=self.external_order_id, symbol=self.symbol_str)

    def cancel(self) -> BinanceClientOrder:
        try:
            self._order = self._client_provider().cancel_order(orderId=self.external_order_id, symbol=self.symbol_str)
        except BinanceAPIException:
            self._logger.error("Failed to cancel order due to BinanceAPIException", exc_info=True)
            return self.cancel()
        self._last_update_time_nanos = self._engine_time_provider.engine_time
        return self

    ### region event handlers

    @override
    def on_enter_order(self, msg: EnterOrderMsg) -> None:
        self._internal_order_id = msg.internal_order_id
        self._venue = msg.venue
        self._symbol = msg.symbol
        self._create_time_nanos = msg.context.engine_time

    @override
    def on_order_entered(self, msg: OrderEnteredMsg) -> None:
        self._external_order_id = msg.external_order_id
        self._fetch(external_order_id=self._external_order_id, symbol=self._symbol.value)

    @override
    def on_reject_enter_order(self, msg: RejectEnterOrderMsg) -> None:
        ...

    @override
    def on_cancel_order(self, msg: CancelOrderMsg) -> None:
        ...

    @override
    def on_order_cancelled(self, msg: OrderCancelledMsg) -> None:
        ...

    @override
    def on_reject_cancel_order(self, msg: RejectCancelOrderMsg) -> None:
        ...

    @override
    def on_fill(self, msg: FillMsg) -> None:
        self._acked_fills[msg.trade_id] = msg

    @override
    def on_order_completed(self, msg: OrderCompletedMsg) -> None:
        ...

    ### end region