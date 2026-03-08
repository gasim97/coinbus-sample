from typing import override

from application.signal.signal.exponentialmovingaveragetradeprice import ExponentialMovingAverageTradePriceSignal
from application.signal.signal.maxtradeprice import MaxTradePriceSignal
from application.signal.signal.meantradelogreturn import MeanTradeLogReturnSignal
from application.signal.signal.meantradeprice import MeanTradePriceSignal
from application.signal.signal.meantradesimplereturn import MeanTradeSimpleReturnSignal
from application.signal.signal.mediantradelogreturn import MedianTradeLogReturnSignal
from application.signal.signal.mediantradeprice import MedianTradePriceSignal
from application.signal.signal.mediantradesimplereturn import MedianTradeSimpleReturnSignal
from application.signal.signal.midtradeprice import MidTradePriceSignal
from application.signal.signal.mintradeprice import MinTradePriceSignal
from application.signal.signal.signal import Signal
from application.signal.signal.weightedmeantradelogreturn import WeightedMeanTradeLogReturnSignal
from application.signal.signal.weightedmeantradeprice import WeightedMeanTradePriceSignal
from application.signal.signal.weightedmeantradesimplereturn import WeightedMeanTradeSimpleReturnSignal
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
from generated.type.marketdata.msg.trade import TradeMsg
from generated.type.signal.enum.signaltype import SignalTypeEnum


class TradePriceSignalsNode(ReadWriteNode, SignalProducerSubscriptionManagerListener):

    _CONFLATION_PERIOD_NS = seconds_to_nanos(1)

    __slots__ = (
        '_signal_subscription_manager',
        '_market_data_manager', 
        '_venue_symbol_last_publish_time',
        '_signals'
    )

    def __init__(self, name: str = "TRDPRICESTATS", environment: EnvironmentEnum = EnvironmentEnum.TEST):
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
        self._market_data_manager.subscribe_trade(callback=self._handle_trade)
        for signal in self._signals.values():
            signal.subscribe_to_messages()
    
    @override
    def on_signal_registered(self, signal_subscription: SignalSubscription) -> None:
        venue_symbol_subscriptions = self._signal_subscription_manager.subscriptions(
            venue=signal_subscription.venue, symbol=signal_subscription.symbol,
        )
        if len(venue_symbol_subscriptions) == 1:
            self._market_data_manager.subscribe_to_symbol(venue=signal_subscription.venue, symbol=signal_subscription.symbol)
    
    def _handle_trade(self, msg: TradeMsg) -> None:
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
            self._init_max_trade_price_signal(),
            self._init_min_trade_price_signal(),
            self._init_mid_trade_price_signal(),
            self._init_mean_trade_price_signal(),
            self._init_weighted_mean_trade_price_signal(),
            self._init_median_trade_price_signal(),
            self._init_mean_trade_simple_return_signal(),
            self._init_weighted_mean_trade_simple_return_signal(),
            self._init_median_trade_simple_return_signal(),
            self._init_mean_trade_log_return_signal(),
            self._init_weighted_mean_trade_log_return_signal(),
            self._init_median_trade_log_return_signal(),
            self._init_exponential_moving_average_trade_price_signal(),
        ]
        return {signal.signal_type: signal for signal in signals}
    
    def _init_max_trade_price_signal(self) -> MaxTradePriceSignal:
        return MaxTradePriceSignal(
            message_sender=super().message_sender,
            subscription_manager=super().subscription_manager,
            signal_subscription_manager=self._signal_subscription_manager,
            market_data_manager=self._market_data_manager,
        )
    
    def _init_min_trade_price_signal(self) -> MinTradePriceSignal:
        return MinTradePriceSignal(
            message_sender=super().message_sender,
            subscription_manager=super().subscription_manager,
            signal_subscription_manager=self._signal_subscription_manager,
            market_data_manager=self._market_data_manager,
        )
    
    def _init_mid_trade_price_signal(self) -> MidTradePriceSignal:
        return MidTradePriceSignal(
            message_sender=super().message_sender,
            subscription_manager=super().subscription_manager,
            signal_subscription_manager=self._signal_subscription_manager,
            market_data_manager=self._market_data_manager,
        )
    
    def _init_mean_trade_price_signal(self) -> MeanTradePriceSignal:
        return MeanTradePriceSignal(
            message_sender=super().message_sender,
            subscription_manager=super().subscription_manager,
            signal_subscription_manager=self._signal_subscription_manager,
            market_data_manager=self._market_data_manager,
        )
    
    def _init_weighted_mean_trade_price_signal(self) -> WeightedMeanTradePriceSignal:
        return WeightedMeanTradePriceSignal(
            message_sender=super().message_sender,
            subscription_manager=super().subscription_manager,
            signal_subscription_manager=self._signal_subscription_manager,
            market_data_manager=self._market_data_manager,
        )
    
    def _init_median_trade_price_signal(self) -> MedianTradePriceSignal:
        return MedianTradePriceSignal(
            message_sender=super().message_sender,
            subscription_manager=super().subscription_manager,
            signal_subscription_manager=self._signal_subscription_manager,
            market_data_manager=self._market_data_manager,
        )
    
    def _init_mean_trade_simple_return_signal(self) -> MeanTradeSimpleReturnSignal:
        return MeanTradeSimpleReturnSignal(
            message_sender=super().message_sender,
            subscription_manager=super().subscription_manager,
            signal_subscription_manager=self._signal_subscription_manager,
            market_data_manager=self._market_data_manager,
        )
    
    def _init_weighted_mean_trade_simple_return_signal(self) -> WeightedMeanTradeSimpleReturnSignal:
        return WeightedMeanTradeSimpleReturnSignal(
            message_sender=super().message_sender,
            subscription_manager=super().subscription_manager,
            signal_subscription_manager=self._signal_subscription_manager,
            market_data_manager=self._market_data_manager,
        )
    
    def _init_median_trade_simple_return_signal(self) -> MedianTradeSimpleReturnSignal:
        return MedianTradeSimpleReturnSignal(
            message_sender=super().message_sender,
            subscription_manager=super().subscription_manager,
            signal_subscription_manager=self._signal_subscription_manager,
            market_data_manager=self._market_data_manager,
        )
    
    def _init_mean_trade_log_return_signal(self) -> MeanTradeLogReturnSignal:
        return MeanTradeLogReturnSignal(
            message_sender=super().message_sender,
            subscription_manager=super().subscription_manager,
            signal_subscription_manager=self._signal_subscription_manager,
            market_data_manager=self._market_data_manager,
        )
    
    def _init_weighted_mean_trade_log_return_signal(self) -> WeightedMeanTradeLogReturnSignal:
        return WeightedMeanTradeLogReturnSignal(
            message_sender=super().message_sender,
            subscription_manager=super().subscription_manager,
            signal_subscription_manager=self._signal_subscription_manager,
            market_data_manager=self._market_data_manager,
        )
    
    def _init_median_trade_log_return_signal(self) -> MedianTradeLogReturnSignal:
        return MedianTradeLogReturnSignal(
            message_sender=super().message_sender,
            subscription_manager=super().subscription_manager,
            signal_subscription_manager=self._signal_subscription_manager,
            market_data_manager=self._market_data_manager,
        )
    
    def _init_exponential_moving_average_trade_price_signal(self) -> ExponentialMovingAverageTradePriceSignal:
        return ExponentialMovingAverageTradePriceSignal(
            message_sender=super().message_sender,
            subscription_manager=super().subscription_manager,
            signal_subscription_manager=self._signal_subscription_manager,
            market_data_manager=self._market_data_manager,
        )