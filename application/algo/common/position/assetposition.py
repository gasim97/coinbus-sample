from dataclasses import dataclass

from common.type.wfloat import WFloat
from generated.type.common.enum.ticker import TickerEnum
from generated.type.common.enum.venue import VenueEnum


@dataclass
class AssetPosition:
    venue: VenueEnum
    ticker: TickerEnum
    position: WFloat = WFloat(value=0, precision=0)
    locked_position: WFloat = WFloat(value=0, precision=0)

    def add_position(self, quantity: WFloat) -> None:
        self.position += quantity
    
    def lock_position(self, quantity: WFloat) -> None:
        self._remove_position(quantity)
        self._add_locked_position(quantity)

    def unlock_position(self, quantity: WFloat) -> None:
        self.remove_locked_position(quantity)
        self.add_position(quantity)

    def remove_locked_position(self, quantity: WFloat) -> None:
        self.locked_position -= quantity
        if self.locked_position < 0:
            self.position += self.locked_position
            self.locked_position -= self.locked_position

    def _add_locked_position(self, quantity: WFloat) -> None:
        self.locked_position += quantity

    def _remove_position(self, quantity: WFloat) -> None:
        self.position -= quantity