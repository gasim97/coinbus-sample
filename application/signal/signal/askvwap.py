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
from generated.type.core.constants import INVALID_PRICE_FLOAT
from generated.type.marketdata.enum.bookside import BookSideEnum
from generated.type.signal.constants import DEFAULT_GRADIENT
from generated.type.signal.enum.signalparametertype import SignalParameterTypeEnum
from generated.type.signal.enum.signaltype import SignalTypeEnum
from generated.type.signal.msg.askvwap import AskVwapMsg
from generated.type.signal.msg.subscribeaskvwap import SubscribeAskVwapMsg


class AskVwapSignal(Signal):

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
            signal_type=SignalTypeEnum.ASK_VWAP, 
        )
        self._market_data_manager = market_data_manager
    
    @override
    def subscribe_to_messages(self) -> None:
        super().subscription_manager.subscribe(
            type=SubscribeAskVwapMsg, callback=self._handle_subscribe_ask_vwap,
        )
    
    def _handle_subscribe_ask_vwap(self, msg: SubscribeAskVwapMsg) -> None:
        super().signal_subscription_manager.register_signal(
            request_id=msg.request_id,
            venue=msg.venue,
            symbol=msg.symbol, 
            signal_type=super().signal_type, 
            parameters=[msg.ask_depth],
            filters=msg.filters,
            tags=msg.tags,
        )    
    
    @override
    @with_memo(key_type=SignalSubscription, item_type=SignalMemoItem)
    def evaluate(self, subscription: SignalSubscription, memo: Memo[SignalSubscription, SignalMemoItem]) -> None:
        depth = subscription.parameter(parameter_type=SignalParameterTypeEnum.BOOK_DEPTH, type=int)
        order_book = self._market_data_manager.order_book(venue=subscription.venue, symbol=subscription.symbol)
        ask_vwap = order_book.side_vwap(side=BookSideEnum.ASKS, depth=depth)
        if ask_vwap == INVALID_PRICE_FLOAT:
            return
        ask_vwap = SignalFiltersApplier.apply(value=ask_vwap, filter_parameters=subscription.filter_parameters)
        signal_memo = memo.get_or_create(key=subscription)
        signal_memo.update(value=ask_vwap)
        with super().message_sender.create(type=AskVwapMsg) as ask_vwap_msg:
            ask_vwap_msg.signal_id = subscription.signal_id
            ask_vwap_msg.ask_depth = order_book.ask_depth
            ask_vwap_msg.vwap = ask_vwap
            ask_vwap_msg.gradient = signal_memo.gradient() or DEFAULT_GRADIENT
            ask_vwap_msg.best_ask = order_book.best_ask.price
            ask_vwap_msg.ask_volume = order_book.side_volume(side=BookSideEnum.ASKS, depth=depth)
            super().message_sender.send(msg=ask_vwap_msg)