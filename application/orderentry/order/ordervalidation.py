from typing import Optional

from application.referencedata.store.symbolinfostore import SymbolInfoStore
from generated.type.common.enum.side import SideEnum
from generated.type.common.enum.symbol import SymbolEnum
from generated.type.common.enum.ticker import TickerEnum
from generated.type.common.enum.venue import VenueEnum
from generated.type.orderentry.constants import INVALID_INTERNAL_ORDER_ID
from generated.type.orderentry.enum.ordertype import OrderTypeEnum
from generated.type.orderentry.enum.timeinforce import TimeInForceEnum
from generated.type.orderentry.msg.enterorder import EnterOrderMsg


def validate_enter_order(msg: EnterOrderMsg, symbol_info_store: SymbolInfoStore) -> Optional[str]:
    symbol_info = symbol_info_store.info(venue=msg.venue, symbol=msg.symbol)
    if symbol_info is None:
        return "Invalid symbol"
    if msg.internal_order_id == INVALID_INTERNAL_ORDER_ID:
        return "Invalid internal order ID"
    if msg.symbol == SymbolEnum.INVALID:
        return "Invalid symbol"
    if msg.venue == VenueEnum.INVALID:
        return "Invalid venue"
    if msg.side == SideEnum.INVALID:
        return "Invalid side"
    if msg.quantity_asset == TickerEnum.INVALID:
        return "Invalid quantity asset"
    if msg.order_type == OrderTypeEnum.INVALID:
        return "Invalid order type"
    if msg.order_type == OrderTypeEnum.LIMIT and msg.time_in_force == TimeInForceEnum.INVALID:
        return "Invalid time in force"
    if msg.order_type == OrderTypeEnum.LIMIT and msg.price == None:
        return "Limit order must have a price"
    if msg.order_type != OrderTypeEnum.MARKET and msg.quantity_asset == symbol_info.quote_asset:
        return "Quote asset quantity can only be used for market orders"
    if msg.order_type != OrderTypeEnum.MARKET and msg.quantity_asset != symbol_info.base_asset:
        return "Base asset quantity must be used for non-market orders"
    return None
