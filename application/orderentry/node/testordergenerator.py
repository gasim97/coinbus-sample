from typing import override

from application.marketdata.store.marketdatamanager import MarketDataManager
from application.orderentry.manager.subscription import OrderHandler, OrderSubscriptionManager
from application.orderentry.order.internalorder import InternalOrder
from application.referencedata.store.symbolinfostore import SymbolInfoStore
from core.node.node import ReadWriteNode
from generated.type.common.enum.side import SideEnum
from generated.type.common.enum.symbol import SymbolEnum
from generated.type.common.enum.venue import VenueEnum
from generated.type.core.enum.environment import EnvironmentEnum
from generated.type.marketdata.msg.depthupdate import DepthUpdateMsg
from generated.type.orderentry.enum.ordertype import OrderTypeEnum
from generated.type.orderentry.enum.timeinforce import TimeInForceEnum
from generated.type.orderentry.msg.enterorder import EnterOrderMsg
from generated.type.orderentry.msg.ordercancelled import OrderCancelledMsg
from generated.type.orderentry.msg.ordercompleted import OrderCompletedMsg
from generated.type.orderentry.msg.orderentered import OrderEnteredMsg
from generated.type.orderentry.msg.rejectcancelorder import RejectCancelOrderMsg
from generated.type.orderentry.msg.rejectenterorder import RejectEnterOrderMsg
from generated.type.orderentry.msg.cancelorder import CancelOrderMsg
from generated.type.orderentry.msg.fill import FillMsg
from generated.type.referencedata.msg.symbolinfo import SymbolInfoMsg
from generated.type.referencedata.msg.symbolinforequest import SymbolInfoRequestMsg


class TestOrderGeneratorNode(ReadWriteNode, OrderHandler[InternalOrder]):

    _VENUE = VenueEnum.BINANCE
    _SYMBOL = SymbolEnum.BTCUSDT
    _QUOTE_QUANTITY = 10

    __slots__ = (
        "_order_subscription_manager",
        "_market_data_manager",
        "_symbol_info_store",
    )

    def __init__(self, name: str = "TESTORDERGENERATOR", environment: EnvironmentEnum = EnvironmentEnum.TEST):
        assert environment == EnvironmentEnum.TEST  # This node is intended to be used in test environments only
        super().__init__(name=name, environment=environment)
        self._order_subscription_manager = OrderSubscriptionManager(
            subscription_manager=super().subscription_manager,
            order_handler=self,
            order_pool=super().pool_manager.pool(type=InternalOrder),
        )
        self._market_data_manager = MarketDataManager(
            pool_manager=super().pool_manager,
            subscription_manager=super().subscription_manager,
            message_sender=super().message_sender,
            engine_time_provider=super().engine_time_provider,
        )
        self._symbol_info_store = SymbolInfoStore(pool_manager=super().pool_manager)
        self._subscribe_to_messages()
    
    @override
    def on_activated(self) -> None:
        self._send_symbol_info_request()
        self._market_data_manager.subscribe_to_symbol(venue=self._VENUE, symbol=self._SYMBOL)
    
    @override
    def on_deactivated(self) -> None:
        ...
    
    def _subscribe_to_messages(self) -> None:
        self._order_subscription_manager.subscribe_mine()
        self._market_data_manager.subscribe_depth_update(callback=self._handle_depth_update)
        super().subscription_manager.subscribe(type=SymbolInfoMsg, callback=self._handle_symbol_info)
    
    def _handle_symbol_info(self, msg: SymbolInfoMsg) -> None:
        self._symbol_info_store.on_symbol_info(msg=msg)
    
    def _handle_depth_update(self, msg: DepthUpdateMsg) -> None:
        if msg.venue != self._VENUE or msg.symbol != self._SYMBOL:
            return
        if len(self._order_subscription_manager.orders) > 0:
            return
        if super().message_sender.has_messages_in_flight:
            return
        order_book = self._market_data_manager.order_book(venue=self._VENUE, symbol=self._SYMBOL)
        if not order_book.active:
            return
        symbol_info = self._symbol_info_store.info(venue=self._VENUE, symbol=self._SYMBOL)
        if symbol_info is None:
            return
        self._send_enter_order(side=SideEnum.BUY, symbol_info=symbol_info)
        self._send_enter_order(side=SideEnum.SELL, symbol_info=symbol_info)

    @override
    def handle_enter_order(self, order: InternalOrder, msg: EnterOrderMsg) -> None:
        ...

    @override
    def handle_order_entered(self, order: InternalOrder, msg: OrderEnteredMsg) -> None:
        ...

    @override
    def handle_reject_enter_order(self, order: InternalOrder, msg: RejectEnterOrderMsg) -> None:
        ...

    @override
    def handle_cancel_order(self, order: InternalOrder, msg: CancelOrderMsg) -> None:
        ...

    @override
    def handle_order_cancelled(self, order: InternalOrder, msg: OrderCancelledMsg) -> None:
        ...

    @override
    def handle_reject_cancel_order(self, order: InternalOrder, msg: RejectCancelOrderMsg) -> None:
        ...

    @override
    def handle_fill(self, order: InternalOrder, msg: FillMsg) -> None:
        ...

    @override
    def handle_order_completed(self, order: InternalOrder, msg: OrderCompletedMsg) -> None:
        ...
    
    def _send_symbol_info_request(self) -> None:
        with super().message_sender.create(type=SymbolInfoRequestMsg) as symbol_info_request_msg:
            symbol_info_request_msg.symbol = self._SYMBOL
            super().message_sender.send(msg=symbol_info_request_msg)
    
    def _send_enter_order(self, side: SideEnum, symbol_info: SymbolInfoMsg) -> None:
        near_touch = self._market_data_manager.order_book(venue=self._VENUE, symbol=self._SYMBOL).near_touch(order_side=side)
        price = self._symbol_info_store.sanitise_price(venue=self._VENUE, symbol=self._SYMBOL, price=near_touch or -1, side=side)
        if price is None:
            self.logger.error(f"Failed to sanitise price for symbol {self._SYMBOL}")
            return

        quantity_asset = symbol_info.base_asset
        quantity = self._symbol_info_store.sanitise_base_asset_quantity(
            venue=self._VENUE, symbol=self._SYMBOL, quantity=self._QUOTE_QUANTITY / price.as_float, price=price.as_float,
        )
        if quantity is None:
            self.logger.error(f"Failed to sanitise quantity for symbol {self._SYMBOL}")
            return

        with super().message_sender.create(type=EnterOrderMsg) as enter_order_msg:
            enter_order_msg.venue = self._VENUE
            enter_order_msg.symbol = self._SYMBOL
            enter_order_msg.side = side
            enter_order_msg.price = price
            enter_order_msg.stop_price = None
            enter_order_msg.quantity = quantity
            enter_order_msg.quantity_asset = quantity_asset
            enter_order_msg.order_type = OrderTypeEnum.LIMIT
            enter_order_msg.time_in_force = TimeInForceEnum.GTC
            super().message_sender.send(msg=enter_order_msg)