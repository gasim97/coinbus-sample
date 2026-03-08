from typing import Any, Optional

from generated.type.account.msg.accountinfo import AccountInfoMsg
from generated.type.account.msg.balance import BalanceMsg
from generated.type.common.enum.ticker import TickerEnum
from generated.type.common.enum.venue import VenueEnum


VENUE = VenueEnum.BINANCE


def balance(balance: dict[str, Any], balance_msg: BalanceMsg) -> Optional[BalanceMsg]:
    balance_msg.ticker = TickerEnum.__members__.get(balance["asset"], TickerEnum.INVALID)
    balance_msg.free = float(balance["free"])
    balance_msg.locked = float(balance["locked"])

    if balance_msg.ticker == TickerEnum.INVALID or (balance_msg.free == 0 and balance_msg.locked == 0):
        balance_msg.release()
        return None
    return balance_msg


def account_info(
    account_info: dict[str, Any],
    account_info_msg: Optional[AccountInfoMsg] = None,
) -> AccountInfoMsg:
    """Maps a Binance account info response to the internal message format"""
    account_info_msg = account_info_msg or AccountInfoMsg()
    account_info_msg.venue = VENUE
    account_info_msg.can_trade = account_info["canTrade"]
    account_info_msg.can_withdraw = account_info["canWithdraw"]
    account_info_msg.can_deposit = account_info["canDeposit"]
    
    balances = account_info["balances"]
    if len(balances) > 0:
        account_info_msg.balances = [
            balance_msg for balance_msg in [
                balance(balance=data, balance_msg=account_info_msg.create_balance_msg()) 
                for data in balances
            ] 
            if balance_msg is not None
        ]

    return account_info_msg 