from collections import defaultdict
from typing import Optional, override

from application.marketdata.store.marketdatamanager import MarketDataManager
from application.orderentry.manager.subscription import OrderSubscriptionManager, PartialOrderHandler
from application.orderentry.order import ordervalidation
from application.orderentry.order.internalorder import InternalOrder
from application.orderentry.utils.commission import Commission
from application.referencedata.store.symbolinfostore import SymbolInfoStore
from common.type.venuesymbolkey import VenueSymbolKey, VenueSymbolKeyStore
from common.type.wfloat import WFloat
from core.node.node import ReadWriteNode
from generated.type.common.enum.side import SideEnum
from generated.type.common.enum.symbol import SymbolEnum
from generated.type.common.enum.venue import VenueEnum
from generated.type.core.enum.environment import EnvironmentEnum
from generated.type.marketdata.msg.depthupdate import DepthUpdateMsg
from generated.type.marketdata.msg.trade import TradeMsg
from generated.type.orderentry.enum.ordertype import OrderTypeEnum
from generated.type.orderentry.enum.orderstatus import OrderStatusEnum
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


class OrderEntrySimulatorNode(ReadWriteNode, PartialOrderHandler[InternalOrder]):

    _COMMISSION_RATE = 0.001

    __slots__ = (
        '_order_subscription_manager',
        '_symbol_info_store',
        '_market_data_manager',
        '_venue_symbol_key_store',
        '_active_orders_by_key',
        '_trade_id_counter',
    )

    def __init__(self, name: str = "ORDERENTRYSIMULATOR", environment: EnvironmentEnum = EnvironmentEnum.TEST):
        super().__init__(name=name, environment=environment)
        self._order_subscription_manager = OrderSubscriptionManager(
            subscription_manager=super().subscription_manager,
            order_handler=self,
            order_pool=super().pool_manager.pool(type=InternalOrder),
            persist_completed_orders=False,
        )
        self._symbol_info_store = SymbolInfoStore(pool_manager=super().pool_manager)
        self._market_data_manager = MarketDataManager(
            pool_manager=super().pool_manager,
            subscription_manager=super().subscription_manager,
            message_sender=super().message_sender,
            engine_time_provider=super().engine_time_provider,
        )
        self._venue_symbol_key_store = VenueSymbolKeyStore(pool_manager=super().pool_manager)
        self._active_orders_by_key: defaultdict[VenueSymbolKey, dict[str, InternalOrder]] = defaultdict(dict)
        self._trade_id_counter: int = 0
        self._subscribe_to_messages()

    @override
    def on_activated(self) -> None:
        ...

    @override
    def on_deactivated(self) -> None:
        for order in list(self._order_subscription_manager.orders.values()):
            if not order.is_complete:
                self._cancel_order(internal_order_id=order.internal_order_id)

    def _subscribe_to_messages(self) -> None:
        self._order_subscription_manager.subscribe_all()
        self._market_data_manager.subscribe_depth_update(callback=self._handle_depth_update)
        self._market_data_manager.subscribe_trade(callback=self._handle_trade)
        super().subscription_manager.subscribe(type=SymbolInfoMsg, callback=self._handle_symbol_info)

    ### region event handlers

    def _handle_symbol_info(self, msg: SymbolInfoMsg) -> None:
        self._symbol_info_store.on_symbol_info(msg=msg)
        key = self._venue_symbol_key_store.get(venue=msg.venue, symbol=msg.symbol)
        pending_orders = list(self._active_orders_by_key[key].values())
        for order in pending_orders:
            if order.is_complete:
                continue
            if order.order_type == OrderTypeEnum.MARKET or self._is_limit_order_executable(order):
                self._simulate_fill(order=order)

    def _handle_depth_update(self, msg: DepthUpdateMsg) -> None:
        key = self._venue_symbol_key_store.get(venue=msg.venue, symbol=msg.symbol)
        active_orders = list(self._active_orders_by_key[key].values())
        for order in active_orders:
            if not order.is_complete and order.order_type == OrderTypeEnum.MARKET:
                self._simulate_fill(order=order)

    def _handle_trade(self, msg: TradeMsg) -> None:
        key = self._venue_symbol_key_store.get(venue=msg.venue, symbol=msg.symbol)
        active_orders = list(self._active_orders_by_key[key].values())
        for order in active_orders:
            if (
                not order.is_complete
                and order.order_type == OrderTypeEnum.LIMIT
                and self._is_limit_order_executable_at_price(order=order, price=msg.price)
            ):
                self._simulate_fill(order=order, fill_price=msg.price)

    @override
    def handle_enter_order(self, order: InternalOrder, msg: EnterOrderMsg) -> None:
        validation_error = ordervalidation.validate_enter_order(msg=msg, symbol_info_store=self._symbol_info_store)
        if validation_error is not None:
            self._send_reject_enter_order(internal_order_id=msg.internal_order_id, reason=validation_error)
            return

        self._send_symbol_reference_data_requests(venue=order.venue, symbol=order.symbol)
        self._market_data_manager.subscribe_to_symbol(venue=order.venue, symbol=order.symbol)
        
        key = self._venue_symbol_key_store.get(venue=order.venue, symbol=order.symbol)
        self._active_orders_by_key[key][order.internal_order_id] = order

        self._send_order_entered(
            internal_order_id=order.internal_order_id, external_order_id=self._next_trade_id(),
        )
        # For market orders or immediately executable limit orders, simulate fills
        if msg.order_type == OrderTypeEnum.MARKET or self._is_limit_order_executable(order):
            self._simulate_fill(order=order, check_in_flight=False)

    @override
    def handle_reject_enter_order(self, order: InternalOrder, msg: RejectEnterOrderMsg) -> None:
        key = self._venue_symbol_key_store.get(venue=order.venue, symbol=order.symbol)
        self._active_orders_by_key[key].pop(order.internal_order_id, None)

    @override
    def handle_cancel_order(self, order: InternalOrder, msg: CancelOrderMsg) -> None:
        self._cancel_order(internal_order_id=msg.internal_order_id)

    @override
    def handle_order_cancelled(self, order: InternalOrder, msg: OrderCancelledMsg) -> None:
        key = self._venue_symbol_key_store.get(venue=order.venue, symbol=order.symbol)
        self._active_orders_by_key[key].pop(order.internal_order_id, None)

    @override
    def handle_fill(self, order: InternalOrder, msg: FillMsg) -> None:
        if order.is_complete or (
            self._symbol_info_store.has_symbol_info(venue=order.venue, symbol=order.symbol)
            and self._is_leaves_quantity_below_step_size(
                order=order, price=order.average_execution_price,
            )
        ):
            self._send_order_completed(order=order)

    @override
    def handle_order_completed(self, order: InternalOrder, msg: OrderCompletedMsg) -> None:
        key = self._venue_symbol_key_store.get(venue=order.venue, symbol=order.symbol)
        self._active_orders_by_key[key].pop(order.internal_order_id, None)

    ### end region

    ### region simulation helpers

    def _next_trade_id(self) -> int:
        self._trade_id_counter += 1
        return self._trade_id_counter

    def _is_limit_order_executable(self, order: InternalOrder) -> bool:
        order_book = self._market_data_manager.order_book(venue=order.venue, symbol=order.symbol)
        if not order_book.active:
            return False
        far_touch = order_book.far_touch(order_side=order.side)
        if far_touch is None:
            return False
        return self._is_limit_order_executable_at_price(order=order, price=far_touch)

    def _is_limit_order_executable_at_price(self, order: InternalOrder, price: float) -> bool:
        if order.price is None:
            return False
        if order.is_buy:
            return order.price.value >= price
        else:
            return order.price.value <= price

    def _is_leaves_quantity_below_step_size(self, order: InternalOrder, price: float) -> bool:
        step_size = self._symbol_info_store.order_step_size(
            venue=order.venue, 
            symbol=order.symbol, 
            price=price, 
            quantity_asset=order.quantity_asset,
        )
        return order.leaves_quantity.as_float < (step_size or 0)

    def _get_far_touch(self, order: InternalOrder) -> Optional[float]:
        order_book = self._market_data_manager.order_book(venue=order.venue, symbol=order.symbol)
        if not order_book.active:
            return None
        return order_book.far_touch(order_side=order.side)

    def _resolve_fill_quantity(
        self,
        order: InternalOrder,
        price: WFloat,
        symbol_info: SymbolInfoMsg,
    ) -> Optional[tuple[WFloat, WFloat]]:
        if order.quantity_asset == symbol_info.base_asset:
            base_asset_quantity = self._symbol_info_store.sanitise_base_asset_quantity(
                venue=order.venue, symbol=order.symbol, quantity=order.leaves_quantity.as_float, price=price.as_float,
            )
            if base_asset_quantity is None:
                return None
            quote_asset_quantity = base_asset_quantity * price.as_float
        else:
            quote_asset_quantity = self._symbol_info_store.sanitise_quote_asset_quantity(  # type: ignore[assignment]
                venue=order.venue, symbol=order.symbol, quantity=order.leaves_quantity.as_float, price=price.as_float,
            )
            if quote_asset_quantity is None:
                return None
            base_asset_quantity = quote_asset_quantity / price.as_float
            
        return base_asset_quantity, quote_asset_quantity

    def _simulate_fill(
        self, 
        order: InternalOrder, 
        fill_price: Optional[float] = None,
        check_in_flight: bool = True,
    ) -> None:
        if check_in_flight and super().message_sender.has_messages_in_flight:
            return
        
        symbol_info = self._symbol_info_store.info(venue=order.venue, symbol=order.symbol)
        if symbol_info is None:
            return
        
        fill_price = fill_price or self._get_far_touch(order=order)
        if fill_price is None:
            return
        
        sanitised_fill_price = self._symbol_info_store.sanitise_price(
            venue=order.venue, symbol=order.symbol, price=fill_price, side=order.side,
        )
        if sanitised_fill_price is None:
            self._send_reject_enter_order(internal_order_id=order.internal_order_id, reason="Failed to sanitise fill price")
            return

        if self._is_leaves_quantity_below_step_size(order=order, price=sanitised_fill_price.as_float):
            self.logger.info(f"Order step size is greater than the remaining quantity")
            return

        quantities = self._resolve_fill_quantity(order=order, price=sanitised_fill_price, symbol_info=symbol_info)
        if quantities is None:
            self._send_reject_enter_order(internal_order_id=order.internal_order_id, reason="Failed to resolve fill quantities")
            return
            
        base_asset_quantity, quote_asset_quantity = quantities

        self._send_fill(
            internal_order_id=order.internal_order_id,
            trade_id=self._next_trade_id(),
            symbol_info=symbol_info,
            side=order.side,
            price=sanitised_fill_price,
            quantity=base_asset_quantity,
            quote_quantity=quote_asset_quantity,
        )

    def _cancel_order(self, internal_order_id: str) -> None:
        order = self._order_subscription_manager.get_order(internal_order_id=internal_order_id)
        if order is None or order.is_complete:
            self._send_reject_cancel_order(
                internal_order_id=internal_order_id,
                reason="Order not found or already completed",
            )
            return

        self._send_order_cancelled(internal_order_id=internal_order_id)

    ### end region

    ### region message senders

    def _send_symbol_reference_data_requests(self, venue: VenueEnum, symbol: SymbolEnum) -> None:
        if not self._symbol_info_store.has_symbol_info(venue=venue, symbol=symbol):
            self._send_symbol_info_request(venue=venue, symbol=symbol)

    def _send_symbol_info_request(self, venue: VenueEnum, symbol: SymbolEnum) -> None:
        with super().message_sender.create(type=SymbolInfoRequestMsg) as symbol_info_request_msg:
            symbol_info_request_msg.venue = venue
            symbol_info_request_msg.symbol = symbol
            super().message_sender.send(msg=symbol_info_request_msg)

    def _send_order_entered(self, internal_order_id: str, external_order_id: int) -> None:
        with super().message_sender.create(type=OrderEnteredMsg) as order_entered_msg:
            order_entered_msg.internal_order_id = internal_order_id
            order_entered_msg.external_order_id = external_order_id
            super().message_sender.send(msg=order_entered_msg)

    def _send_reject_enter_order(self, internal_order_id: str, reason: str) -> None:
        self.logger.error(f"Rejecting order {internal_order_id} with reason: {reason}")
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

    def _send_fill(
        self,
        internal_order_id: str,
        trade_id: int,
        symbol_info: SymbolInfoMsg,
        side: SideEnum,
        price: WFloat,
        quantity: WFloat,
        quote_quantity: WFloat,
    ) -> None:
        commission_asset, commission = Commission.calculate(
            symbol_info=symbol_info, 
            side=side, 
            base_quantity=quantity, 
            quote_quantity=quote_quantity,
            commission_rate=self._COMMISSION_RATE,
        )

        with super().message_sender.create(type=FillMsg) as fill_msg:
            fill_msg.internal_order_id = internal_order_id
            fill_msg.trade_id = trade_id
            fill_msg.price = price
            fill_msg.quantity = quantity
            fill_msg.quote_quantity = quote_quantity
            fill_msg.commission = commission
            fill_msg.commission_asset = commission_asset
            fill_msg.is_maker = False
            fill_msg.is_best_match = True
            super().message_sender.send(msg=fill_msg)

    def _send_order_completed(self, order: InternalOrder) -> None:
        with super().message_sender.create(type=OrderCompletedMsg) as order_completed_msg:
            order_completed_msg.internal_order_id = order.internal_order_id
            order_completed_msg.status = OrderStatusEnum.FILLED
            order_completed_msg.executed_quantity = order.executed_quantity.as_str
            order_completed_msg.executed_quote_quantity = order.executed_quote_quantity.as_str
            order_completed_msg.working_time = order.last_update_time_nanos - order.create_time_nanos
            super().message_sender.send(msg=order_completed_msg)

    ### end region