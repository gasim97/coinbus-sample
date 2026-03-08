from generated.type.common.enum.venue import VenueEnum
from typing import override

from core.node.node import ReadWriteNode
from generated.type.common.enum.symbol import SymbolEnum
from generated.type.core.enum.environment import EnvironmentEnum
from generated.type.marketdata.msg.depthupdate import DepthUpdateMsg
from generated.type.marketdata.msg.subscribedepthupdate import SubscribeDepthUpdateMsg
from generated.type.marketdata.msg.subscribetrade import SubscribeTradeMsg
from generated.type.marketdata.msg.testmarketdataconsumerconfig import TestMarketDataConsumerConfigMsg
from generated.type.marketdata.msg.trade import TradeMsg


class TestMarketDataConsumerNode(ReadWriteNode):

    def __init__(self, name: str = "MARKETDATACONSUMER", environment: EnvironmentEnum = EnvironmentEnum.TEST):
        super().__init__(name=name, environment=environment)
        self._subscribe_to_config_messages()
        self._subscribe_to_messages()
    
    @override
    def on_activated(self) -> None:
        ...
    
    @override
    def on_deactivated(self) -> None:
        ...
    
    def _subscribe_to_config_messages(self) -> None:
        super().subscription_manager.subscribe_config(type=TestMarketDataConsumerConfigMsg, callback=self._handle_test_market_data_consumer_config_msg)
    
    def _subscribe_to_messages(self) -> None:
        super().subscription_manager.subscribe(type=DepthUpdateMsg, callback=self._handle_depth_update_msg)
        super().subscription_manager.subscribe(type=TradeMsg, callback=self._handle_trade_msg)
    
    def _handle_test_market_data_consumer_config_msg(self, msg: TestMarketDataConsumerConfigMsg) -> None:
        if msg.symbols is None:
            return
        for symbol in msg.symbols:
            self._send_subscribe_depth_update(symbol=symbol, venue=msg.venue)
            self._send_subscribe_trade(symbol=symbol, venue=msg.venue)
    
    def _handle_depth_update_msg(self, msg: DepthUpdateMsg) -> None:
        self.logger.debug(f"Got depth update msg: {msg}")
    
    def _handle_trade_msg(self, msg: TradeMsg) -> None:
        self.logger.debug(f"Got trade msg: {msg}")
    
    def _send_subscribe_depth_update(self, symbol: SymbolEnum, venue: VenueEnum) -> None:
        with super().message_sender.create(type=SubscribeDepthUpdateMsg) as subscribe_depth_update_msg:
            subscribe_depth_update_msg.symbol = symbol
            subscribe_depth_update_msg.venue = venue
            super().message_sender.send(msg=subscribe_depth_update_msg)
    
    def _send_subscribe_trade(self, symbol: SymbolEnum, venue: VenueEnum) -> None:
        with super().message_sender.create(type=SubscribeTradeMsg) as subscribe_trade_msg:
            subscribe_trade_msg.symbol = symbol
            subscribe_trade_msg.venue = venue
            super().message_sender.send(msg=subscribe_trade_msg)