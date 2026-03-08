from typing import Optional

from generated.type.account.msg.accountinfo import AccountInfoMsg
from generated.type.account.msg.balance import BalanceMsg
from generated.type.common.enum.ticker import TickerEnum
from generated.type.common.enum.venue import VenueEnum
from generated.type.core.constants import INVALID_QUANTITY_FLOAT


class AccountInfoStore:
    """Stores and provides access to account information."""

    __slots__ = ('_venue_account_info')

    def __init__(self):
        self._venue_account_info: dict[VenueEnum, AccountInfoMsg] = {}

    def on_account_info(self, msg: AccountInfoMsg) -> None:
        if self.has_account_info(venue=msg.venue):
            self._venue_account_info[msg.venue].copy_from(other=msg)
        else:
            self._venue_account_info[msg.venue] = msg.clone()
    
    def has_account_info(self, venue: VenueEnum) -> bool:
        return venue in self._venue_account_info
    
    def get_account_info(self, venue: VenueEnum) -> Optional[AccountInfoMsg]:
        return self._venue_account_info.get(venue)
    
    def get_balance(self, venue: VenueEnum, ticker: TickerEnum) -> Optional[BalanceMsg]:
        account_info = self.get_account_info(venue=venue)
        if account_info is None:
            return None
        
        for balance in account_info.balances:
            if balance.ticker == ticker:
                return balance
        return None
    
    def get_free_balance(self, venue: VenueEnum, ticker: TickerEnum) -> float:
        balance = self.get_balance(venue=venue, ticker=ticker)
        if balance is None:
            return INVALID_QUANTITY_FLOAT
        return balance.free or INVALID_QUANTITY_FLOAT
    
    def get_locked_balance(self, venue: VenueEnum, ticker: TickerEnum) -> float:
        balance = self.get_balance(venue=venue, ticker=ticker)
        if balance is None:
            return INVALID_QUANTITY_FLOAT
        return balance.locked or INVALID_QUANTITY_FLOAT 