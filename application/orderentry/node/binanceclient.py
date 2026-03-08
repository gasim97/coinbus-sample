from typing import Any, Optional, override

from binance.client import Client  # type: ignore[import-untyped]
from binance.exceptions import BinanceAPIException  # type: ignore[import-untyped]

from application.orderentry.manager.subscription import LambdaOrderFilter, OrderSubscriptionManager, PartialOrderHandler
from application.orderentry.order import ordervalidation
from application.orderentry.order.binanceclientorder import BinanceClientOrder
from application.referencedata.store.symbolinfostore import SymbolInfoStore
from common.type.wfloat import WFloat
from common.utils.time import millis_to_nanos, seconds_to_millis
from core.node.callbackscheduler import ScheduledCallback
from core.node.node import ReadWriteNode
from generated.type.common.enum.symbol import SymbolEnum
from generated.type.common.enum.ticker import TickerEnum
from generated.type.common.enum.venue import VenueEnum
from generated.type.core.enum.environment import EnvironmentEnum
from generated.type.orderentry.constants import INVALID_EXTERNAL_ORDER_ID
from generated.type.orderentry.enum.ordertype import OrderTypeEnum
from generated.type.orderentry.msg.cancelorder import CancelOrderMsg
from generated.type.orderentry.msg.enterorder import EnterOrderMsg
from generated.type.orderentry.msg.fill import FillMsg
from generated.type.orderentry.msg.ordercancelled import OrderCancelledMsg
from generated.type.orderentry.msg.ordercompleted import OrderCompletedMsg
from generated.type.orderentry.msg.orderentered import OrderEnteredMsg
from generated.type.orderentry.msg.rejectcancelorder import RejectCancelOrderMsg
from generated.type.orderentry.msg.rejectenterorder import RejectEnterOrderMsg
from generated.type.referencedata.msg.symbolinfo import SymbolInfoMsg
from generated.type.referencedata.msg.symbolinforequest import SymbolInfoRequestMsg


class BinanceOrderEntryClientNode(ReadWriteNode, PartialOrderHandler[BinanceClientOrder], ScheduledCallback):

    _POLL_LIVE_ORDER_FREQUENCY_NS = millis_to_nanos(100)
    _ORDER_REQUEST_VALIDITY_WINDOW_MS = seconds_to_millis(10)

    __slots__ = (
        '_symbol_info_store',
        '_enter_order_msgs',
        '_live_orders',
        '_pending_in_message_queue',
        '_pending_symbol_info_in_message_queue',
        '_poll_live_order_callback_id',
        # on activation
        '_client',
        '_order_subscription_manager',
    )

    def __init__(self, name: str = "ORDERENTRYCLIENT", environment: EnvironmentEnum = EnvironmentEnum.TEST):
        super().__init__(name=name, environment=environment)
        self._symbol_info_store: SymbolInfoStore = SymbolInfoStore(pool_manager=super().pool_manager)
        self._entered_order_ids: set[str] = set()
        self._live_orders: dict[str, BinanceClientOrder] = {}
        self._pending_in_message_queue: list[EnterOrderMsg | CancelOrderMsg] = []
        self._pending_symbol_info_in_message_queue: dict[SymbolEnum, list[EnterOrderMsg]] = {}
        self._poll_live_order_callback_id: Optional[int] = None
    
    @override
    def on_activated(self) -> None:
        self._client = super().resources.binance_client
        self._order_subscription_manager = self._create_order_subscription_manager()
        self._poll_live_order_callback_id = self._schedule_poll_live_order()
        self._subscribe_to_messages()
    
    @override
    def on_deactivated(self) -> None:
        if self._poll_live_order_callback_id is not None:
            super().callback_scheduler.cancel(self._poll_live_order_callback_id)
            self._poll_live_order_callback_id = None
        for internal_order_id in self._live_orders:
            self._cancel_order(internal_order_id=internal_order_id)

    def _create_order_subscription_manager(self) -> OrderSubscriptionManager:
        return self._order_subscription_manager or OrderSubscriptionManager(
            subscription_manager=super().subscription_manager, 
            order_handler=self, 
            order_pool=super().pool_manager.create_local_pool(
                type=BinanceClientOrder, 
                object_instantiator=lambda: BinanceClientOrder(
                    logger=self.logger, 
                    client_provider=lambda: self._client,
                    engine_time_provider=self.engine_time_provider,
                    order_request_validity_window_ms=self._ORDER_REQUEST_VALIDITY_WINDOW_MS,
                )
            ),
            order_filter=LambdaOrderFilter(lambda order: order.venue == VenueEnum.BINANCE),
        )
    
    def _schedule_poll_live_order(self) -> int:
        return self._poll_live_order_callback_id or super().callback_scheduler.schedule_at_fixed_rate(
            callback=self, frequency_nanos=self._POLL_LIVE_ORDER_FREQUENCY_NS,
        )
    
    @override
    def _on_scheduled_time(self, callback_id: int) -> None:
        self._handle_poll_live_order(callback_id=callback_id)
    
    def _subscribe_to_messages(self) -> None:
        self._order_subscription_manager.subscribe_all()
        super().subscribe_no_messages_in_flight(callback=self._handle_no_messages_in_flight)
        super().subscription_manager.subscribe(type=SymbolInfoMsg, callback=self._handle_symbol_info)
    
    ### region scheduled callback handlers
    
    def _handle_poll_live_order(self, callback_id: int) -> None:
        if super().message_sender.has_messages_in_flight or len(self._live_orders) == 0:
            return
        most_stale_order = min(self._live_orders.values(), key=lambda order: order.last_update_time_nanos or 0)
        self._poll_live_order(internal_order_id=most_stale_order.internal_order_id)
    
    ### end region
        
    ### region event handlers

    def _handle_no_messages_in_flight(self) -> None:
        if len(self._pending_in_message_queue):
            pending_message = self._pending_in_message_queue.pop(0)
            if isinstance(pending_message, EnterOrderMsg):
                order = self._order_subscription_manager.get_order(internal_order_id=pending_message.internal_order_id)
                if order is not None:
                    self.handle_enter_order(order=order, msg=pending_message)
            elif isinstance(pending_message, CancelOrderMsg):
                order = self._order_subscription_manager.get_order(internal_order_id=pending_message.internal_order_id)
                if order is not None:
                    self.handle_cancel_order(order=order, msg=pending_message)
            else:
                self.logger.error(f"Message with unknown type in pending message group ignored: {pending_message}")
            super().pool_manager.pool(type=type(pending_message)).release(object=pending_message)
    
    def _handle_symbol_info(self, msg: SymbolInfoMsg) -> None:
        self._symbol_info_store.on_symbol_info(msg=msg)
        for pending_msg in self._pending_symbol_info_in_message_queue.pop(msg.symbol, []):
            order = self._order_subscription_manager.get_order(internal_order_id=pending_msg.internal_order_id)
            if order is not None:
                self.handle_enter_order(order=order, msg=pending_msg)

    @override
    def handle_enter_order(self, order: BinanceClientOrder, msg: EnterOrderMsg) -> None:
        symbol_info = self._symbol_info_store.info(venue=msg.venue, symbol=msg.symbol)
        if symbol_info is None:
            self._pending_symbol_info_in_message_queue.setdefault(msg.symbol, []).append(msg)
            self._send_symbol_info_request(symbol=msg.symbol)
            return
        if super().message_sender.has_messages_in_flight:
            self._pending_in_message_queue.append(super().pool_manager.pool(type=EnterOrderMsg).get().copy_from(other=msg))
            return
        if not self._validate_enter_order(msg=msg):
            return
        self._entered_order_ids.add(order.internal_order_id)
        self._place_order(msg=msg, symbol_info=symbol_info)

    @override
    def handle_order_entered(self, order: BinanceClientOrder, msg: OrderEnteredMsg) -> None:
        self._live_orders[order.internal_order_id] = order
        self._evaluate_order_update(order=order)

    @override
    def handle_cancel_order(self, order: BinanceClientOrder, msg: CancelOrderMsg) -> None:
        if (
            super().message_sender.has_messages_in_flight 
            or self._poll_live_order(internal_order_id=order.internal_order_id)
        ):
            self._pending_in_message_queue.append(super().pool_manager.pool(type=CancelOrderMsg).get().copy_from(other=msg))
            return
        self._cancel_order(internal_order_id=order.internal_order_id)

    @override
    def handle_order_cancelled(self, order: BinanceClientOrder, msg: OrderCancelledMsg) -> None:
        self._on_order_no_longer_live(internal_order_id=order.internal_order_id)

    @override
    def handle_order_completed(self, order: BinanceClientOrder, msg: OrderCompletedMsg) -> None:
        self._on_order_no_longer_live(internal_order_id=order.internal_order_id)
    
    ### end region
    
    ### region validators

    def _validate_enter_order(self, msg: EnterOrderMsg) -> bool:
        if msg.internal_order_id in self._entered_order_ids:
            self._send_reject_enter_order(internal_order_id=msg.internal_order_id, reason="Duplicate internal order ID")
            return False
        validation_error = ordervalidation.validate_enter_order(msg=msg, symbol_info_store=self._symbol_info_store)
        if validation_error is not None:
            self._send_reject_enter_order(internal_order_id=msg.internal_order_id, reason=validation_error)
            return False
        return True
    
    ### end region

    ### region order action helpers

    def _place_order(self, msg: EnterOrderMsg, symbol_info: SymbolInfoMsg) -> None:
        try:
            order = self._client.create_order(
                newClientOrderId=msg.internal_order_id,
                symbol=msg.symbol.value,
                side=msg.side.value,
                quantity=msg.quantity.as_str if msg.quantity_asset == symbol_info.base_asset else None,
                quoteOrderQty=msg.quantity.as_str if msg.quantity_asset == symbol_info.quote_asset else None,
                price=msg.price.as_str if msg.price else None,
                stopPrice=msg.stop_price.as_str if msg.stop_price else None,
                type=msg.order_type.value,
                timeInForce=msg.time_in_force.value if msg.order_type == OrderTypeEnum.LIMIT else None,
                recvWindow=self._ORDER_REQUEST_VALIDITY_WINDOW_MS,
            )
            self._send_order_entered(
                internal_order_id=msg.internal_order_id, 
                external_order_id=order.get("orderId", INVALID_EXTERNAL_ORDER_ID),
            )
        except BinanceAPIException:
            self.logger.error("Failed to place order due to BinanceAPIException", exc_info=True)
            self._send_reject_enter_order(
                internal_order_id=msg.internal_order_id, reason="Failed to place order due to BinanceAPIException",
            )
    
    def _cancel_order(self, internal_order_id: str) -> None:
        client_order = self._live_orders.get(internal_order_id)
        if client_order is None:
            self._send_reject_cancel_order(internal_order_id=internal_order_id, reason="Unknown/Complete order")
            return
        client_order.cancel()
        if not client_order.is_cancelled:
            self._send_reject_cancel_order(internal_order_id=internal_order_id, reason="Venue rejected cancel")
            return
        self._send_order_cancelled(internal_order_id=internal_order_id)
    
    def _on_order_no_longer_live(self, internal_order_id: str) -> None:
        if internal_order_id in self._live_orders:
            self._live_orders.pop(internal_order_id)
    
    def _poll_live_order(self, internal_order_id: str) -> bool:
        order = self._live_orders.get(internal_order_id)
        if order is None:
            return False
        order.fetch()
        return self._evaluate_order_update(order=order)
    
    def _evaluate_order_update(self, order: BinanceClientOrder) -> bool:
        for new_fill in order.unacked_fills:
            self._send_fill(internal_order_id=order.internal_order_id, fill=new_fill)
        if order.is_complete:
            self._send_order_completed(order=order)
        has_update = len(order.unacked_fills) > 0 or order.is_complete
        return has_update
    
    ### end region

    ### region message senders

    def _send_symbol_info_request(self, symbol: SymbolEnum) -> None:
        with super().message_sender.create(type=SymbolInfoRequestMsg) as symbol_info_request_msg:
            symbol_info_request_msg.symbol = symbol
            super().message_sender.send(msg=symbol_info_request_msg)
    
    def _send_order_entered(self, internal_order_id: str, external_order_id: int) -> None:
        with super().message_sender.create(type=OrderEnteredMsg) as order_entered_msg:
            order_entered_msg.internal_order_id = internal_order_id
            order_entered_msg.external_order_id = external_order_id
            super().message_sender.send(msg=order_entered_msg)
    
    def _send_reject_enter_order(self, internal_order_id: str, reason: str) -> None:
        with super().message_sender.create(type=RejectEnterOrderMsg) as reject_enter_order_msg:
            reject_enter_order_msg.internal_order_id = internal_order_id
            reject_enter_order_msg.reason = reason
            super().message_sender.send(msg=reject_enter_order_msg)
    
    def _send_order_cancelled(self, internal_order_id: str) -> None:
        with super().message_sender.create(type=OrderCancelledMsg) as order_cancelled_msg:
            order_cancelled_msg.internal_order_id = internal_order_id
            super().message_sender.send(msg=order_cancelled_msg)
    
    def _send_reject_cancel_order(self, internal_order_id: str, reason: str) -> None:
        with super().message_sender.create(type=RejectCancelOrderMsg) as reject_cancel_order_msg:
            reject_cancel_order_msg.internal_order_id = internal_order_id
            reject_cancel_order_msg.reason = reason
            super().message_sender.send(msg=reject_cancel_order_msg)
    
    def _send_order_completed(self, order: BinanceClientOrder) -> None:
        with super().message_sender.create(type=OrderCompletedMsg) as order_completed_msg:
            order_completed_msg.internal_order_id = order.internal_order_id
            order_completed_msg.status = order.status
            order_completed_msg.executed_quantity = str(order.executed_quantity)
            order_completed_msg.executed_quote_quantity = str(order.executed_quote_quantity)
            order_completed_msg.working_time = order.working_time_nanos
            super().message_sender.send(msg=order_completed_msg)
    
    def _send_fill(self, internal_order_id: str, fill: dict[str, Any]) -> None:
        with super().message_sender.create(type=FillMsg) as fill_msg:
            fill_msg.internal_order_id = internal_order_id
            fill_msg.trade_id = fill["id"]
            fill_msg.price = WFloat.from_string(value=fill["price"])
            fill_msg.quantity = WFloat.from_string(value=fill["qty"])
            fill_msg.quote_quantity = WFloat.from_string(value=fill["quoteQty"])
            fill_msg.commission = WFloat.from_string(value=fill["commission"])
            fill_msg.commission_asset = TickerEnum[fill["commissionAsset"]]
            fill_msg.is_maker = fill["isMaker"]
            fill_msg.is_best_match = fill["isBestMatch"]
            super().message_sender.send(msg=fill_msg)
    
    ### end region