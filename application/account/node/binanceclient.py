from typing import Any, override

from application.account.utils import binanceadapter
from core.node.node import ReadWriteNode
from generated.type.account.msg.accountinfo import AccountInfoMsg
from generated.type.account.msg.accountinforequest import AccountInfoRequestMsg
from generated.type.account.msg.rejectaccountinforequest import RejectAccountInfoRequestMsg
from generated.type.core.constants import INVALID_REQUEST_ID
from generated.type.core.enum.environment import EnvironmentEnum


class BinanceAccountClientNode(ReadWriteNode):
    """Node that handles Binance account data requests and responses"""

    __slots__ = ('_client')

    def __init__(self, name: str = "ACCOUNTCLIENT", environment: EnvironmentEnum = EnvironmentEnum.TEST):
        super().__init__(name=name, environment=environment)
        self._subscribe_to_messages()
    
    @override
    def on_activated(self) -> None:
        self._client = super().resources.binance_client
        self._request_account_info(request_id=INVALID_REQUEST_ID)

    @override
    def on_deactivated(self) -> None:
        ...
    
    def _subscribe_to_messages(self) -> None:
        super().subscription_manager.subscribe(type=AccountInfoRequestMsg, callback=self._handle_account_info_request)
    
    def _handle_account_info_request(self, msg: AccountInfoRequestMsg) -> None:
        if msg.venue == binanceadapter.VENUE:
            self._request_account_info(request_id=msg.request_id)

    def _request_account_info(self, request_id: str) -> None:
        try:
            account_info = self._client.get_account()
        except Exception:
            self.logger.error(f"Failed to get account info", exc_info=True)
            self._send_reject_account_info_request(request_id=request_id, reason="Failed to get account info")
            return
        if account_info is None:
            self._send_reject_account_info_request(request_id=request_id, reason="Binance response is None")
            return
        self._send_account_info(request_id=request_id, account_info=account_info)
    
    def _send_account_info(self, request_id: str, account_info: dict[str, Any]) -> None:
        with super().message_sender.create(type=AccountInfoMsg) as account_info_msg:
            try:
                account_info_msg = binanceadapter.account_info(account_info=account_info, account_info_msg=account_info_msg)
            except Exception:
                self.logger.error(f"Unable to parse binance response: {account_info}", exc_info=True)
                self._send_reject_account_info_request(request_id=request_id, reason="Unable to parse binance response")
                return
            account_info_msg.request_id = request_id
            super().message_sender.send(msg=account_info_msg)
    
    def _send_reject_account_info_request(self, request_id: str, reason: str) -> None:
        with super().message_sender.create(type=RejectAccountInfoRequestMsg) as reject_account_info_request_msg:
            reject_account_info_request_msg.request_id = request_id
            reject_account_info_request_msg.reason = reason
            super().message_sender.send(msg=reject_account_info_request_msg) 