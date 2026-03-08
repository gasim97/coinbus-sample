from typing import Any, Callable, override

from application.marketdata.utils import binanceadapter, validator
from common.utils.time import millis_to_nanos
from core.node.callbackscheduler import ScheduledCallback
from core.node.node import ReadWriteNode
from generated.type.common.enum.symbol import SymbolEnum
from generated.type.core.enum.environment import EnvironmentEnum
from generated.type.marketdata.msg.cleardepthupdate import ClearDepthUpdateMsg
from generated.type.marketdata.msg.depthupdate import DepthUpdateMsg
from generated.type.marketdata.msg.subscribedepthupdate import SubscribeDepthUpdateMsg
from generated.type.marketdata.msg.subscribetrade import SubscribeTradeMsg
from generated.type.marketdata.msg.trade import TradeMsg


class _StartSocketCallback(ScheduledCallback):

    __slots__ = ('_start_socket_callback', '_symbol')

    def __init__(self, start_socket_callback: Callable[[SymbolEnum], None], symbol: SymbolEnum):
        self._start_socket_callback = start_socket_callback
        self._symbol = symbol
    
    @override
    def _on_scheduled_time(self, callback_id: int) -> None:
        self._start_socket_callback(self._symbol)


class BinanceMarketDataStreamClientNode(ReadWriteNode):

    _RE_START_SOCKER_DELAY_NS = millis_to_nanos(200)

    __slots__ = (
        '_active_depth_update_sockets', 
        '_active_trade_sockets', 
        # on activation
        '_socket_manager', 
    )

    def __init__(self, name: str = "MARKETDATASTREAMCLIENT", environment: EnvironmentEnum = EnvironmentEnum.TEST):
        super().__init__(name=name, environment=environment)
        self._active_depth_update_sockets: dict[SymbolEnum, str] = {}
        self._active_trade_sockets: dict[SymbolEnum, str] = {}
        self._subscribe_to_messages()
    
    @override
    def on_activated(self) -> None:
        self._socket_manager = super().resources.binance_threaded_web_socket_manager
        self._socket_manager.start()
    
    @override
    def on_deactivated(self) -> None:
        self._socket_manager.stop()

    def _subscribe_to_messages(self) -> None:
        super().subscription_manager.subscribe(type=SubscribeDepthUpdateMsg, callback=self._handle_subscribe_depth_update)
        super().subscription_manager.subscribe(type=SubscribeTradeMsg, callback=self._handle_subscribe_trade)
    
    def _handle_subscribe_depth_update(self, msg: SubscribeDepthUpdateMsg) -> None:
        if msg.venue != binanceadapter.VENUE:
            return
        if msg.symbol in self._active_depth_update_sockets:
            return
        self._subscribe_to_depth_updates(symbol=msg.symbol)
    
    def _handle_subscribe_trade(self, msg: SubscribeTradeMsg) -> None:
        if msg.venue != binanceadapter.VENUE:
            return
        if msg.symbol in self._active_trade_sockets:
            return
        self._subscribe_to_trades(symbol=msg.symbol)
    
    def _subscribe_to_depth_updates(self, symbol: SymbolEnum) -> None:
        if symbol in self._active_depth_update_sockets:
            return
        self._active_depth_update_sockets[symbol] = (
            self._socket_manager.start_depth_socket(
                callback=lambda depth_update: self._handle_binance_depth_update(msg=depth_update, symbol=symbol), 
                symbol=symbol.value,
            )
        )
    
    def _subscribe_to_trades(self, symbol: SymbolEnum) -> None:
        if symbol in self._active_trade_sockets:
            return
        self._active_trade_sockets[symbol] = (
            self._socket_manager.start_trade_socket(
                callback=lambda trade: self._handle_binance_trade(msg=trade, symbol=symbol), 
                symbol=symbol.value,
            )
        )
    
    def _handle_binance_depth_update(self, msg: dict[str, Any], symbol: SymbolEnum) -> None:
        num_chunks = binanceadapter.get_depth_update_chunks_count(depth_update=msg)
        if num_chunks is None:
            self.logger.error(f"Failed to determine number of chunks for depth update message: {msg}")
            self._restart_depth_updates_socket(symbol=symbol)
            return
        with super().message_sender.create(type=DepthUpdateMsg) as depth_update_msg: 
            for chunk_index in range(num_chunks):
                if (
                    binanceadapter.depth_update_stream(
                        depth_update=msg, 
                        chunk_index=chunk_index, 
                        num_chunks=num_chunks, 
                        depth_update_msg=depth_update_msg
                    ) is None 
                    or not validator.is_depth_update_valid(msg=depth_update_msg)
                ):
                    self.logger.error(f"Failed to parse depth update message chunk {chunk_index}: {msg}")
                    self._restart_depth_updates_socket(symbol=symbol)
                    continue
                super().message_sender.send(msg=depth_update_msg)
    
    def _handle_binance_trade(self, msg: dict[str, Any], symbol: SymbolEnum) -> None:
        with super().message_sender.create(type=TradeMsg) as trade_msg:
            if binanceadapter.trade_stream(trade=msg, trade_msg=trade_msg) is None:
                # TODO: This error occurs when more than 100 trade messages are received by the 
                # websocket manager in a short period of time. This is a known issue with the 
                # python-binance library. Consider removing dependency on the library and using FIX 
                # to receive market data.
                self.logger.error(f"Failed to parse trade message: {msg}")
                self._restart_trades_socket(symbol=symbol)
                return
            super().message_sender.send(msg=trade_msg)
    
    def _restart_depth_updates_socket(self, symbol: SymbolEnum) -> None:
        if symbol not in self._active_depth_update_sockets:
            return
        self.logger.warning(f"Restarting depth updates socket for symbol: {symbol.value}")
        self._socket_manager.stop_socket(self._active_depth_update_sockets.pop(symbol))
        self._send_clear_depth_update(symbol=symbol)
        self.callback_scheduler.schedule(
            callback=_StartSocketCallback(start_socket_callback=self._subscribe_to_depth_updates, symbol=symbol), 
            delay_nanos=self._RE_START_SOCKER_DELAY_NS,
        )
    
    def _restart_trades_socket(self, symbol: SymbolEnum) -> None:
        if symbol not in self._active_trade_sockets:
            return
        self.logger.warning(f"Restarting trades socket for symbol: {symbol.value}")
        self._socket_manager.stop_socket(self._active_trade_sockets.pop(symbol))
        self.callback_scheduler.schedule(
            callback=_StartSocketCallback(start_socket_callback=self._subscribe_to_trades, symbol=symbol),
            delay_nanos=self._RE_START_SOCKER_DELAY_NS,
        )
    
    def _send_clear_depth_update(self, symbol: SymbolEnum) -> None:
        if symbol not in self._active_depth_update_sockets:
            return
        self.logger.warning(f"Clearing depth updates for symbol: {symbol.value}")
        with super().message_sender.create(type=ClearDepthUpdateMsg) as clear_depth_update_msg:
            clear_depth_update_msg.venue = binanceadapter.VENUE
            clear_depth_update_msg.symbol = symbol
            super().message_sender.send(msg=clear_depth_update_msg)
