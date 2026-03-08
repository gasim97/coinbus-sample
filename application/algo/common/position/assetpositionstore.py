from __future__ import annotations

from typing import Generic, TypeVar, override

from application.algo.common.position.assetposition import AssetPosition
from application.orderentry.manager.subscription import PartialOrderHandler
from application.orderentry.order.internalorder import InternalOrder
from application.referencedata.store.symbolinfostore import SymbolInfoStore
from common.type.venuetickerkey import VenueTickerKey, VenueTickerKeyStore
from common.type.wfloat import WFloat
from common.utils.symbolhelper import is_base_asset
from generated.type.common.enum.side import SideEnum
from generated.type.common.enum.ticker import TickerEnum
from generated.type.common.enum.venue import VenueEnum
from generated.type.orderentry.msg.enterorder import EnterOrderMsg
from generated.type.orderentry.msg.fill import FillMsg
from generated.type.orderentry.msg.ordercancelled import OrderCancelledMsg
from generated.type.orderentry.msg.ordercompleted import OrderCompletedMsg
from generated.type.orderentry.msg.rejectenterorder import RejectEnterOrderMsg


O = TypeVar("O", bound=InternalOrder)


class AssetPositionStore(Generic[O]):

    __slots__ = ('_venue_ticker_key_store', '_asset_positions', '_order_handler')

    def __init__(self, venue_ticker_key_store: VenueTickerKeyStore, symbol_info_store: SymbolInfoStore) -> None:
        self._venue_ticker_key_store = venue_ticker_key_store
        self._asset_positions: dict[VenueTickerKey, AssetPosition] = {}
        self._order_handler = AssetPositionOrderHandler[O](
            asset_position_store=self, symbol_info_store=symbol_info_store,
        )

    @property
    def order_handler(self) -> AssetPositionOrderHandler[O]:
        return self._order_handler

    def has_position(self, venue: VenueEnum, ticker: TickerEnum) -> bool:
        venue_ticker_key = self._venue_ticker_key_store.get(venue=venue, ticker=ticker)
        return venue_ticker_key in self._asset_positions and self.position(venue=venue, ticker=ticker).position > 0

    def position(self, venue: VenueEnum, ticker: TickerEnum) -> AssetPosition:
        venue_ticker_key = self._venue_ticker_key_store.get(venue=venue, ticker=ticker)
        return self._asset_positions.setdefault(venue_ticker_key, AssetPosition(venue=venue, ticker=ticker))


class AssetPositionOrderHandler(PartialOrderHandler[O]):

    __slots__ = ('_asset_position_store', '_symbol_info_store')

    def __init__(self, asset_position_store: AssetPositionStore, symbol_info_store: SymbolInfoStore) -> None:
        self._asset_position_store = asset_position_store
        self._symbol_info_store = symbol_info_store

    def _get_asset_position(self, order: O, versus_asset_position: bool = False) -> AssetPosition:
        symbol_info = self._symbol_info_store.info(venue=order.venue, symbol=order.symbol)
        if symbol_info is None:
            raise ValueError(f"Symbol info not found for symbol {order.symbol}, unable to update asset position")
        is_buy = order.side == SideEnum.BUY
        ticker = symbol_info.quote_asset if is_buy != versus_asset_position else symbol_info.base_asset
        return self._asset_position_store.position(venue=order.venue, ticker=ticker)

    def _get_leaves_quantity(self, order: O) -> WFloat:
        return order.leaves_quote_asset_quantity if order.side == SideEnum.BUY else order.leaves_base_asset_quantity

    @override
    def handle_enter_order(self, order: O, msg: EnterOrderMsg) -> None:
        asset_position = self._get_asset_position(order=order)
        quantity = order.quote_asset_quantity if order.side == SideEnum.BUY else order.base_asset_quantity
        asset_position.lock_position(quantity=quantity)

    @override
    def handle_reject_enter_order(self, order: O, msg: RejectEnterOrderMsg) -> None:
        asset_position = self._get_asset_position(order=order)
        asset_position.unlock_position(quantity=self._get_leaves_quantity(order))

    @override
    def handle_order_cancelled(self, order: O, msg: OrderCancelledMsg) -> None:
        asset_position = self._get_asset_position(order=order)
        asset_position.unlock_position(quantity=self._get_leaves_quantity(order))

    @override
    def handle_fill(self, order: O, msg: FillMsg) -> None:
        is_buy = order.side == SideEnum.BUY
        is_commission_base_asset = is_base_asset(symbol=order.symbol, asset=msg.commission_asset)
        asset_position = self._get_asset_position(order=order)
        versus_asset_position = self._get_asset_position(order=order, versus_asset_position=True)
        commission = (
            msg.commission 
            if is_buy == is_commission_base_asset 
            else msg.commission / msg.price
            if is_buy
            else msg.commission * msg.price
        )
        quantity = msg.quote_quantity if is_buy else msg.quantity
        versus_quantity = msg.quantity if is_buy else msg.quote_quantity
        asset_position.remove_locked_position(quantity=quantity)
        versus_asset_position.add_position(quantity=versus_quantity - commission)

    @override
    def handle_order_completed(self, order: O, msg: OrderCompletedMsg) -> None:
        asset_position = self._get_asset_position(order=order)
        asset_position.unlock_position(quantity=self._get_leaves_quantity(order))