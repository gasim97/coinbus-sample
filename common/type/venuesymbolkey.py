from typing import Any, Self

from common.utils.pool import ObjectPoolManager, Poolable
from generated.type.common.enum.symbol import SymbolEnum
from generated.type.common.enum.venue import VenueEnum


class VenueSymbolKey(Poolable):
    
    __slots__ = ('_venue', '_symbol')

    def __init__(self):
        self.clear()
    
    @property
    def venue(self) -> VenueEnum:
        return self._venue
    
    @property
    def symbol(self) -> SymbolEnum:
        return self._symbol
    
    def clear(self) -> Self:
        self._venue = VenueEnum.INVALID
        self._symbol = SymbolEnum.INVALID
        return self

    def set(self, venue: VenueEnum, symbol: SymbolEnum) -> Self:
        self._venue = venue
        self._symbol = symbol
        return self
    
    def __hash__(self) -> int:
        return hash((self._venue, self._symbol))
    
    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, VenueSymbolKey):
            return False
        return self._venue == other._venue and self._symbol == other._symbol


class VenueSymbolKeyStore:

    __slots__ = ('_key_pool', '_key_store', '_lookup_key')

    def __init__(self, pool_manager: ObjectPoolManager):
        self._key_pool = pool_manager.create_local_pool(type=VenueSymbolKey)
        self._key_store: dict[VenueSymbolKey, VenueSymbolKey] = {}
        self._lookup_key = self._key_pool.get()
    
    def get(self, venue: VenueEnum, symbol: SymbolEnum) -> VenueSymbolKey:
        if self._lookup_key.set(venue=venue, symbol=symbol) not in self._key_store:
            key = self._key_pool.get().set(venue=venue, symbol=symbol)
            self._key_store[key] = key
        return self._key_store[self._lookup_key]
    