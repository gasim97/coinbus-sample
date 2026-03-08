from typing import override

from application.signal.signal.askvwap import AskVwapSignal
from application.signal.signal.bidvwap import BidVwapSignal
from application.signal.signal.bookvwap import BookVwapSignal
from application.signal.signal.signal import Signal
from application.signal.store.marketdatamanager import TradePriceSignalsMarketDataManager
from application.signal.subscription.producermanager import (
    SignalProducerSubscriptionManager,
    SignalProducerSubscriptionManagerListener,
)
from application.signal.subscription.subscription import SignalSubscription
from common.type.venuesymbolkey import VenueSymbolKey
from common.utils.time import seconds_to_nanos
from core.node.node import ReadWriteNode
from generated.type.core.enum.environment import EnvironmentEnum
from generated.type.marketdata.msg.depthupdate import DepthUpdateMsg
from generated.type.signal.enum.signaltype import SignalTypeEnum


class BookSignalsNode(ReadWriteNode, SignalProducerSubscriptionManagerListener):

    _CONFLATION_PERIOD_NS = seconds_to_nanos(1)

    __slots__ = (
        '_signal_subscription_manager',
        '_market_data_manager', 
        '_venue_symbol_last_publish_time',
        '_signals',
    )
    
    def __init__(self, name: str = "BOOKSTATS", environment: EnvironmentEnum = EnvironmentEnum.TEST):
        super().__init__(name=name, environment=environment)
        self._signal_subscription_manager = SignalProducerSubscriptionManager(
            node_name=name,
            message_sender=super().message_sender,
            listener=self,
            pool_manager=super().pool_manager,
        )
        self._market_data_manager = TradePriceSignalsMarketDataManager(
            pool_manager=super().pool_manager,
            subscription_manager=super().subscription_manager,
            message_sender=super().message_sender,
            engine_time_provider=super().engine_time_provider,
        )
        self._venue_symbol_last_publish_time: dict[VenueSymbolKey, int] = {}
        self._signals = self._init_signals()
        self._subscribe_to_messages()
    
    @override
    def on_activated(self) -> None:
        ...
    
    @override
    def on_deactivated(self) -> None:
        ...
    
    def _subscribe_to_messages(self) -> None:
        self._market_data_manager.subscribe_depth_update(callback=self._handle_depth_update)
        for signal in self._signals.values():
            signal.subscribe_to_messages()
    
    @override
    def on_signal_registered(self, signal_subscription: SignalSubscription) -> None:
        venue_symbol_subscriptions = self._signal_subscription_manager.subscriptions(
            venue=signal_subscription.venue, symbol=signal_subscription.symbol,
        )
        if len(venue_symbol_subscriptions) == 1:
            self._market_data_manager.subscribe_to_symbol(venue=signal_subscription.venue, symbol=signal_subscription.symbol)

    def _handle_depth_update(self, msg: DepthUpdateMsg) -> None:
        if not msg.is_final_chunk:
            return
        order_book = self._market_data_manager.order_book(venue=msg.venue, symbol=msg.symbol)
        if not order_book.active:
            return
        venue_symbol_key = self._signal_subscription_manager.venue_symbol_key(venue=msg.venue, symbol=msg.symbol)
        venue_symbol_last_publish_time = self._venue_symbol_last_publish_time.get(venue_symbol_key, 0)
        if self._CONFLATION_PERIOD_NS > (super().engine_time_provider.engine_time - venue_symbol_last_publish_time):
            return
        for subscription in self._signal_subscription_manager.subscriptions(venue=msg.venue, symbol=msg.symbol):
            signal = self._signals.get(subscription.signal_type)
            if signal is not None:
                signal.evaluate(subscription=subscription)
        self._venue_symbol_last_publish_time[venue_symbol_key] = super().engine_time_provider.engine_time

    def _init_signals(self) -> dict[SignalTypeEnum, Signal]:
        signals = [
            self._init_book_vwap_signal(),
            self._init_bid_vwap_signal(),
            self._init_ask_vwap_signal(),
        ]
        return {signal.signal_type: signal for signal in signals}
    
    def _init_book_vwap_signal(self) -> Signal:
        return BookVwapSignal(
            message_sender=super().message_sender,
            subscription_manager=super().subscription_manager,
            signal_subscription_manager=self._signal_subscription_manager,
            market_data_manager=self._market_data_manager,
        )
    
    def _init_bid_vwap_signal(self) -> Signal:
        return BidVwapSignal(
            message_sender=super().message_sender,
            subscription_manager=super().subscription_manager,
            signal_subscription_manager=self._signal_subscription_manager,
            market_data_manager=self._market_data_manager,
        )
    
    def _init_ask_vwap_signal(self) -> Signal:
        return AskVwapSignal(
            message_sender=super().message_sender,
            subscription_manager=super().subscription_manager,
            signal_subscription_manager=self._signal_subscription_manager,
            market_data_manager=self._market_data_manager,
        )