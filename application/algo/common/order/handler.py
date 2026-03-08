from logging import Logger
from typing import Optional, TypeVar, override

from application.algo.common.order.callback import OrderCallback
from application.algo.common.order.sender import OrderSender
from application.algo.common.position.assetpositionstore import AssetPositionStore
from application.orderentry.manager.subscription import OrderHandler
from application.orderentry.order.internalorder import InternalOrder
from application.referencedata.store.symbolinfostore import SymbolInfoStore
from core.node.callbackscheduler import CallbackScheduler
from generated.type.common.enum.symbol import SymbolEnum
from generated.type.common.enum.venue import VenueEnum
from generated.type.orderentry.msg.cancelorder import CancelOrderMsg
from generated.type.orderentry.msg.enterorder import EnterOrderMsg
from generated.type.orderentry.msg.fill import FillMsg
from generated.type.orderentry.msg.ordercancelled import OrderCancelledMsg
from generated.type.orderentry.msg.ordercompleted import OrderCompletedMsg
from generated.type.orderentry.msg.orderentered import OrderEnteredMsg
from generated.type.orderentry.msg.rejectcancelorder import RejectCancelOrderMsg
from generated.type.orderentry.msg.rejectenterorder import RejectEnterOrderMsg


O = TypeVar('O', bound=InternalOrder)


class LoggingOrderHandler(OrderHandler[O]):

    __slots__ = (
        '_logger', 
        '_callback_scheduler', 
        '_asset_position_store', 
        '_symbol_info_store', 
        '_order_sender',
        '_max_order_lifetime_nanos',
    )

    def __init__(
        self,
        logger: Logger,
        callback_scheduler: CallbackScheduler,
        asset_position_store: AssetPositionStore,
        symbol_info_store: SymbolInfoStore,
        order_sender: OrderSender,
        max_order_lifetime_nanos: Optional[int] = None,
    ):
        self._logger = logger
        self._callback_scheduler = callback_scheduler
        self._asset_position_store = asset_position_store
        self._symbol_info_store = symbol_info_store
        self._order_sender = order_sender
        self._max_order_lifetime_nanos = max_order_lifetime_nanos

    def _log_position(self, venue: VenueEnum, symbol: SymbolEnum, is_final: bool = False) -> None:
        symbol_info = self._symbol_info_store.info(venue=venue, symbol=symbol)
        if symbol_info is None:
            return
        self._logger.info(
            f"{is_final and 'Final ' or ''}Position - "
            f"Base: {self._asset_position_store.position(venue=venue, ticker=symbol_info.base_asset).position}, "
            f"Quote: {self._asset_position_store.position(venue=venue, ticker=symbol_info.quote_asset).position}"
        )

    @override
    def handle_enter_order(self, order: O, msg: EnterOrderMsg) -> None:
        if self._max_order_lifetime_nanos is not None:
            self._callback_scheduler.schedule(
                callback=OrderCallback[O](callback=self._order_sender.send_cancel_order, order=order),
                delay_nanos=self._max_order_lifetime_nanos,
            )
        self._logger.info(f"Enter order: {order.side.value} {order.symbol.value} {order.quantity}@{order.price}")
        self._log_position(venue=order.venue, symbol=order.symbol)
        self._asset_position_store.order_handler.handle_enter_order(order=order, msg=msg)

    @override
    def handle_order_entered(self, order: O, msg: OrderEnteredMsg) -> None:
        ...

    @override
    def handle_reject_enter_order(self, order: O, msg: RejectEnterOrderMsg) -> None:
        self._asset_position_store.order_handler.handle_reject_enter_order(order=order, msg=msg)
        self._logger.info(f"Reject enter order: {order.side.value} {order.symbol.value} {order.quantity}@{order.price}")
        self._log_position(venue=order.venue, symbol=order.symbol)

    @override
    def handle_cancel_order(self, order: O, msg: CancelOrderMsg) -> None:
        ...

    @override
    def handle_order_cancelled(self, order: O, msg: OrderCancelledMsg) -> None:
        self._asset_position_store.order_handler.handle_order_cancelled(order=order, msg=msg)
        self._logger.info(f"Order cancelled: {order.side.value} {order.symbol.value} {order.quantity}@{order.price}")
        self._log_position(venue=order.venue, symbol=order.symbol)

    @override
    def handle_reject_cancel_order(self, order: O, msg: RejectCancelOrderMsg) -> None:
        ...

    @override
    def handle_fill(self, order: O, msg: FillMsg) -> None:
        self._asset_position_store.order_handler.handle_fill(order=order, msg=msg)
        self._logger.info(f"Fill: {order.side.value} {order.symbol.value} {msg.quantity}@{msg.price}")
        self._log_position(venue=order.venue, symbol=order.symbol)

    @override
    def handle_order_completed(self, order: O, msg: OrderCompletedMsg) -> None:
        self._asset_position_store.order_handler.handle_order_completed(order=order, msg=msg)
        self._logger.info(f"Order completed: {order.side.value} {order.symbol.value} {order.quantity}@{order.average_execution_price}")
        self._log_position(venue=order.venue, symbol=order.symbol, is_final=True)