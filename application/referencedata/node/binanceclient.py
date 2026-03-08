from typing import Any, override

from application.referencedata.utils import binanceadapter
from core.node.node import ReadWriteNode
from generated.type.common.enum.symbol import SymbolEnum
from generated.type.core.enum.environment import EnvironmentEnum
from generated.type.referencedata.msg.rejectsymbolinforequest import RejectSymbolInfoRequestMsg
from generated.type.referencedata.msg.rejectsymbolpricerequest import RejectSymbolPriceRequestMsg
from generated.type.referencedata.msg.symbolinfo import SymbolInfoMsg
from generated.type.referencedata.msg.symbolinforequest import SymbolInfoRequestMsg
from generated.type.referencedata.msg.symbolprice import SymbolPriceMsg
from generated.type.referencedata.msg.symbolpricerequest import SymbolPriceRequestMsg


class BinanceReferenceDataClientNode(ReadWriteNode):

    __slots__ = ('_client')

    def __init__(self, name: str = "REFERENCEDATACLIENT", environment: EnvironmentEnum = EnvironmentEnum.TEST):
        super().__init__(name=name, environment=environment)
        self._subscribe_to_messages()
    
    @override
    def on_activated(self) -> None:
        self._client = super().resources.binance_client
    
    @override
    def on_deactivated(self) -> None:
        ...
    
    def _subscribe_to_messages(self) -> None:
        super().subscription_manager.subscribe(type=SymbolInfoRequestMsg, callback=self._handle_symbol_info_request)
        super().subscription_manager.subscribe(type=SymbolPriceRequestMsg, callback=self._handle_symbol_price_request)
    
    def _handle_symbol_info_request(self, msg: SymbolInfoRequestMsg) -> None:
        if msg.venue != binanceadapter.VENUE:
            return
        symbol_info = self._client.get_symbol_info(symbol=msg.symbol.value)
        if symbol_info is None:
            self._send_reject_symbol_info_request(
                request_id=msg.request_id, symbol=msg.symbol, reason="Binance response is None",
            )
            return
        self._send_symbol_info(request_id=msg.request_id, symbol=msg.symbol, symbol_info=symbol_info)

    def _handle_symbol_price_request(self, msg: SymbolPriceRequestMsg) -> None:
        if msg.venue != binanceadapter.VENUE:
            return
        symbol_price = self._client.get_symbol_ticker(symbol=msg.symbol.value)
        if symbol_price is None:
            self._send_reject_symbol_price_request(
                request_id=msg.request_id, symbol=msg.symbol, reason="Binance response is None",
            )
            return
        self._send_symbol_price(request_id=msg.request_id, symbol=msg.symbol, symbol_price=symbol_price)
    
    def _send_symbol_info(self, request_id: str, symbol: SymbolEnum, symbol_info: dict[str, Any]) -> None:
        with super().message_sender.create(type=SymbolInfoMsg) as symbol_info_msg:
            try:
                symbol_info_msg = binanceadapter.symbol_info(
                    symbol=symbol, symbol_info=symbol_info, symbol_info_msg=symbol_info_msg,
                )
            except Exception:
                self._logger.error(f"Unable to parse binance response: {symbol_info}", exc_info=True)
                self._send_reject_symbol_info_request(
                    request_id=request_id, symbol=symbol, reason="Unable to parse binance response",
                )
                return
            symbol_info_msg.request_id = request_id
            super().message_sender.send(msg=symbol_info_msg)

    def _send_symbol_price(self, request_id: str, symbol: SymbolEnum, symbol_price: dict[str, Any]) -> None:
        with super().message_sender.create(type=SymbolPriceMsg) as symbol_price_msg:
            try:
                symbol_price_msg = binanceadapter.symbol_price(
                    symbol=symbol, symbol_price=symbol_price, symbol_price_msg=symbol_price_msg,
                )
            except Exception:
                self._logger.error(f"Unable to parse binance response: {symbol_price}", exc_info=True)
                self._send_reject_symbol_price_request(
                    request_id=request_id, symbol=symbol, reason="Unable to parse binance response",
                )
                return
            symbol_price_msg.request_id = request_id
            super().message_sender.send(msg=symbol_price_msg)
    
    def _send_reject_symbol_info_request(self, request_id: str, symbol: SymbolEnum, reason: str) -> None:
        with super().message_sender.create(type=RejectSymbolInfoRequestMsg) as reject_symbol_info_request_msg:
            reject_symbol_info_request_msg.request_id = request_id
            reject_symbol_info_request_msg.venue = binanceadapter.VENUE
            reject_symbol_info_request_msg.symbol = symbol
            reject_symbol_info_request_msg.reason = reason
            super().message_sender.send(msg=reject_symbol_info_request_msg)

    def _send_reject_symbol_price_request(self, request_id: str, symbol: SymbolEnum, reason: str) -> None:
        with super().message_sender.create(type=RejectSymbolPriceRequestMsg) as reject_symbol_price_request_msg:
            reject_symbol_price_request_msg.request_id = request_id
            reject_symbol_price_request_msg.venue = binanceadapter.VENUE
            reject_symbol_price_request_msg.symbol = symbol
            reject_symbol_price_request_msg.reason = reason
            super().message_sender.send(msg=reject_symbol_price_request_msg)