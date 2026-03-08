from typing import Optional

from application.referencedata.utils.numericvalueconstraints import (
    NotionalConstraints, 
    PriceConstraints, 
    QuantityConstraints,
)
from common.type.venuesymbolkey import VenueSymbolKey, VenueSymbolKeyStore
from common.type.wfloat import WFloat
from common.utils.pool import ObjectPoolManager
from generated.type.common.enum.side import SideEnum
from generated.type.common.enum.symbol import SymbolEnum
from generated.type.common.enum.ticker import TickerEnum
from generated.type.common.enum.venue import VenueEnum
from generated.type.referencedata.msg.symbolinfo import SymbolInfoMsg


class SymbolInfoStore:

    __slots__ = ('_pool_manager', '_key_store', '_info_by_key', '_price_constraints', '_quantity_constraints', '_notional_constraints')

    def __init__(self, pool_manager: ObjectPoolManager):
        self._pool_manager = pool_manager
        self._key_store = VenueSymbolKeyStore(pool_manager=pool_manager)
        self._info_by_key: dict[VenueSymbolKey, SymbolInfoMsg] = {}
        self._price_constraints = PriceConstraints()
        self._quantity_constraints = QuantityConstraints()
        self._notional_constraints = NotionalConstraints()

    def on_symbol_info(self, msg: SymbolInfoMsg) -> None:
        msg_clone = msg.clone()
        key = self._key_store.get(venue=msg_clone.venue, symbol=msg_clone.symbol)
        self._info_by_key[key] = msg_clone
    
    def has_symbol_info(self, venue: VenueEnum, symbol: SymbolEnum) -> bool:
        return self._key_store.get(venue=venue, symbol=symbol) in self._info_by_key
    
    def info(self, venue: VenueEnum, symbol: SymbolEnum) -> Optional[SymbolInfoMsg]:
        return self._info_by_key.get(self._key_store.get(venue=venue, symbol=symbol))
    
    def sanitise_base_asset_quantity(
        self, 
        venue: VenueEnum,
        symbol: SymbolEnum, 
        quantity: float, 
        price: float,
    ) -> Optional[WFloat]:
        return self._sanitise_quantity(
            venue=venue, symbol=symbol, quantity=quantity, price=price, is_base_asset=True,
        )
    
    def sanitise_quote_asset_quantity(
        self, 
        venue: VenueEnum,
        symbol: SymbolEnum, 
        quantity: float, 
        price: float,
    ) -> Optional[WFloat]:
        return self._sanitise_quantity(
            venue=venue, symbol=symbol, quantity=quantity, price=price, is_base_asset=False,
        )
    
    def sanitise_quantity_for_market_order(
        self, 
        venue: VenueEnum,
        symbol: SymbolEnum, 
        quantity: float, 
        quantity_asset: TickerEnum,
    ) -> Optional[WFloat]:
        symbol_info = self.info(venue=venue, symbol=symbol)
        if symbol_info is None:
            return None
        if quantity_asset == symbol_info.base_asset:
            return self._quantity_constraints.init_base_asset(symbol_info=symbol_info).apply(quantity=quantity)
        return self._quantity_constraints.init_market_order_quote_asset(symbol_info=symbol_info).apply(quantity=quantity)
    
    def sanitise_price(
        self, 
        venue: VenueEnum,
        symbol: SymbolEnum, 
        price: float,
        side: SideEnum,
    ) -> Optional[WFloat]:
        symbol_info = self.info(venue=venue, symbol=symbol)
        if symbol_info is None:
            return None
        return self._price_constraints.init_price(symbol_info=symbol_info).apply(price=price, side=side)
    
    def order_step_size(
        self, 
        venue: VenueEnum, 
        symbol: SymbolEnum, 
        price: float, 
        quantity_asset: TickerEnum,
    ) -> Optional[WFloat]:
        symbol_info = self.info(venue=venue, symbol=symbol)
        if symbol_info is None:
            return None
        if quantity_asset == symbol_info.base_asset:
            return self._quantity_constraints.init_base_asset(symbol_info=symbol_info).step_size
        return self._quantity_constraints.init_quote_asset(symbol_info=symbol_info, price=price).step_size

    def _sanitise_quantity(
        self,
        venue: VenueEnum,
        symbol: SymbolEnum,
        quantity: float, 
        price: float,
        is_base_asset: bool,
    ) -> Optional[WFloat]:
        symbol_info = self.info(venue=venue, symbol=symbol)
        if symbol_info is None:
            return None
        if is_base_asset:
            self._quantity_constraints.init_base_asset(symbol_info=symbol_info)
            self._notional_constraints.init_base_asset(symbol_info=symbol_info)
        else:
            self._quantity_constraints.init_quote_asset(symbol_info=symbol_info, price=price)
            self._notional_constraints.init_quote_asset(symbol_info=symbol_info, price=price)
        constrained_quantity = self._quantity_constraints.apply(quantity=quantity)
        if constrained_quantity is None:
            return None
        if not self._notional_constraints.apply(quantity=constrained_quantity.as_float, price=price):
            return None
        return constrained_quantity