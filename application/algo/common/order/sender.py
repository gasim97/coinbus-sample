from application.referencedata.store.symbolinfostore import SymbolInfoStore
from common.type.wfloat import WFloat
from common.utils.number import decimal_places_from_float
from core.node.messagesender import MessageSender
from generated.type.common.enum.side import SideEnum
from generated.type.common.enum.symbol import SymbolEnum
from generated.type.common.enum.ticker import TickerEnum
from generated.type.common.enum.venue import VenueEnum
from generated.type.orderentry.enum.ordertype import OrderTypeEnum
from generated.type.orderentry.enum.timeinforce import TimeInForceEnum
from generated.type.orderentry.msg.cancelorder import CancelOrderMsg
from generated.type.orderentry.msg.enterorder import EnterOrderMsg


class OrderSender:
    
    __slots__ = ('_message_sender', '_symbol_info_store')

    def __init__(self, message_sender: MessageSender, symbol_info_store: SymbolInfoStore):
        self._message_sender = message_sender
        self._symbol_info_store = symbol_info_store
    
    def send_limit_order(
        self, venue: VenueEnum, symbol: SymbolEnum, side: SideEnum, base_asset_quantity: float, price: float,
    ) -> None:
        symbol_info = self._symbol_info_store.info(venue=venue, symbol=symbol)
        if symbol_info is None:
            return
        sanitised_price = self._symbol_info_store.sanitise_price(venue=venue, symbol=symbol, price=price, side=side)
        if sanitised_price is None:
            return
        sanitised_quantity = self._symbol_info_store.sanitise_base_asset_quantity(
            venue=venue, symbol=symbol, quantity=base_asset_quantity, price=sanitised_price.as_float,
        )
        if sanitised_quantity is None:
            return
        with self._message_sender.create(type=EnterOrderMsg) as enter_order_msg:
            enter_order_msg.venue = venue
            enter_order_msg.symbol = symbol
            enter_order_msg.side = side
            enter_order_msg.quantity = sanitised_quantity
            enter_order_msg.quantity_asset = symbol_info.base_asset
            enter_order_msg.price = sanitised_price
            enter_order_msg.order_type = OrderTypeEnum.LIMIT
            enter_order_msg.time_in_force = TimeInForceEnum.GTC
            self._message_sender.send(msg=enter_order_msg)
    
    def send_market_order(
        self, venue: VenueEnum, symbol: SymbolEnum, side: SideEnum, quantity: float, quantity_asset: TickerEnum, reference_price: float,
    ) -> None:
        sanitised_quantity = self._symbol_info_store.sanitise_quantity_for_market_order(
            venue=venue, symbol=symbol, quantity=quantity, quantity_asset=quantity_asset,
        )
        if sanitised_quantity is None:
            return
        with self._message_sender.create(type=EnterOrderMsg) as enter_order_msg:
            enter_order_msg.venue = venue
            enter_order_msg.symbol = symbol
            enter_order_msg.side = side
            enter_order_msg.reference_price = WFloat(value=reference_price, precision=decimal_places_from_float(value=reference_price))
            enter_order_msg.quantity = sanitised_quantity
            enter_order_msg.quantity_asset = quantity_asset
            enter_order_msg.order_type = OrderTypeEnum.MARKET
            self._message_sender.send(msg=enter_order_msg)
    
    def send_cancel_order(self, internal_order_id: str) -> None:
        with self._message_sender.create(type=CancelOrderMsg) as cancel_order_msg:
            cancel_order_msg.internal_order_id = internal_order_id
            self._message_sender.send(msg=cancel_order_msg)