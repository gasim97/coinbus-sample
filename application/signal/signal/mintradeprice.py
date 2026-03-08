from typing import override

from application.signal.filter.applier import SignalFiltersApplier
from application.signal.signal.signal import Signal
from application.signal.store.marketdatamanager import TradePriceSignalsMarketDataManager
from application.signal.subscription.producermanager import SignalProducerSubscriptionManager
from application.signal.subscription.subscription import SignalSubscription
from application.signal.utils.memo import SignalMemoItem
from common.utils.memo import Memo, with_memo
from core.node.messagesender import MessageSender
from core.node.subscription import SubscriptionManager
from generated.type.signal.constants import DEFAULT_GRADIENT
from generated.type.signal.enum.signalparametertype import SignalParameterTypeEnum
from generated.type.signal.enum.signaltype import SignalTypeEnum
from generated.type.signal.msg.mintradeprice import MinTradePriceMsg
from generated.type.signal.msg.subscribemintradeprice import SubscribeMinTradePriceMsg


class MinTradePriceSignal(Signal):

    __slots__ = ('_market_data_manager')
    
    def __init__(
        self, 
        message_sender: MessageSender,
        subscription_manager: SubscriptionManager, 
        signal_subscription_manager: SignalProducerSubscriptionManager,
        market_data_manager: TradePriceSignalsMarketDataManager,
    ):
        super().__init__(
            message_sender=message_sender, 
            subscription_manager=subscription_manager, 
            signal_subscription_manager=signal_subscription_manager,
            signal_type=SignalTypeEnum.MIN_PRICE, 
        )
        self._market_data_manager = market_data_manager
    
    @override
    def subscribe_to_messages(self) -> None:
        super().subscription_manager.subscribe(
            type=SubscribeMinTradePriceMsg, callback=self._handle_subscribe_min_trade_price,
        )
    
    def _handle_subscribe_min_trade_price(self, msg: SubscribeMinTradePriceMsg) -> None:
        super().signal_subscription_manager.register_signal(
            request_id=msg.request_id,
            venue=msg.venue,
            symbol=msg.symbol, 
            signal_type=super().signal_type, 
            parameters=[msg.window_nanos],
            filters=msg.filters,
            tags=msg.tags,
        )    
    
    @override
    @with_memo(key_type=SignalSubscription, item_type=SignalMemoItem)
    def evaluate(self, subscription: SignalSubscription, memo: Memo[SignalSubscription, SignalMemoItem]) -> None:
        window_nanos = subscription.parameter(parameter_type=SignalParameterTypeEnum.WINDOW_NS, type=int)
        if window_nanos is None:
            return
        trade_ledger = self._market_data_manager.trade_ledger(venue=subscription.venue, symbol=subscription.symbol)
        price = trade_ledger.min_price(window_nanos=window_nanos)
        if price is None:
            return
        price = SignalFiltersApplier.apply(value=price, filter_parameters=subscription.filter_parameters)
        signal_memo = memo.get_or_create(key=subscription)
        signal_memo.update(value=price)
        with super().message_sender.create(type=MinTradePriceMsg) as min_trade_price_msg:
            min_trade_price_msg.signal_id = subscription.signal_id
            min_trade_price_msg.last_trade_time_nanos = trade_ledger.latest_time
            min_trade_price_msg.price = price
            min_trade_price_msg.gradient = signal_memo.gradient() or DEFAULT_GRADIENT
            super().message_sender.send(msg=min_trade_price_msg)