from typing import Any, Self

from common.utils.pool import ObjectPoolManager, Poolable
from generated.type.common.enum.ticker import TickerEnum
from generated.type.common.enum.venue import VenueEnum


class VenueTickerKey(Poolable):
    
    __slots__ = ('_venue', '_ticker')

    def __init__(self):
        self.clear()
    
    @property
    def venue(self) -> VenueEnum:
        return self._venue
    
    @property
    def ticker(self) -> TickerEnum:
        return self._ticker
    
    def clear(self) -> Self:
        self._venue = VenueEnum.INVALID
        self._ticker = TickerEnum.INVALID
        return self

    def set(self, venue: VenueEnum, ticker: TickerEnum) -> Self:
        self._venue = venue
        self._ticker = ticker
        return self
    
    def __hash__(self) -> int:
        return hash((self._venue, self._ticker))
    
    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, VenueTickerKey):
            return False
        return self._venue == other._venue and self._ticker == other._ticker


class VenueTickerKeyStore:

    __slots__ = ('_key_pool', '_key_store', '_lookup_key')

    def __init__(self, pool_manager: ObjectPoolManager):
        self._key_pool = pool_manager.create_local_pool(type=VenueTickerKey)
        self._key_store: dict[VenueTickerKey, VenueTickerKey] = {}
        self._lookup_key = self._key_pool.get()
    
    def get(self, venue: VenueEnum, ticker: TickerEnum) -> VenueTickerKey:
        if self._lookup_key.set(venue=venue, ticker=ticker) not in self._key_store:
            key = self._key_pool.get().set(venue=venue, ticker=ticker)
            self._key_store[key] = key
        return self._key_store[self._lookup_key]
    