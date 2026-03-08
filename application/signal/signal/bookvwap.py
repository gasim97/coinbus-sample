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
from generated.type.signal.constants import DEFAULT_GRADIENT
from generated.type.signal.enum.signalparametertype import SignalParameterTypeEnum
from generated.type.signal.enum.signaltype import SignalTypeEnum
from generated.type.signal.msg.bookvwap import BookVwapMsg
from generated.type.signal.msg.subscribebookvwap import SubscribeBookVwapMsg


class BookVwapSignal(Signal):

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
            signal_type=SignalTypeEnum.BOOK_VWAP, 
        )
        self._market_data_manager = market_data_manager
    
    @override
    def subscribe_to_messages(self) -> None:
        super().subscription_manager.subscribe(
            type=SubscribeBookVwapMsg, callback=self._handle_subscribe_book_vwap,
        )
    
    def _handle_subscribe_book_vwap(self, msg: SubscribeBookVwapMsg) -> None:
        super().signal_subscription_manager.register_signal(
            request_id=msg.request_id,
            venue=msg.venue,
            symbol=msg.symbol, 
            signal_type=super().signal_type, 
            parameters=[msg.book_depth],
            filters=msg.filters,
            tags=msg.tags,
        )    
    
    @override
    @with_memo(key_type=SignalSubscription, item_type=SignalMemoItem)
    def evaluate(self, subscription: SignalSubscription, memo: Memo[SignalSubscription, SignalMemoItem]) -> None:
        depth = subscription.parameter(parameter_type=SignalParameterTypeEnum.BOOK_DEPTH, type=int)
        order_book = self._market_data_manager.order_book(venue=subscription.venue, symbol=subscription.symbol)
        book_vwap = order_book.vwap(depth=depth)
        if book_vwap == INVALID_PRICE_FLOAT:
            return
        book_vwap = SignalFiltersApplier.apply(value=book_vwap, filter_parameters=subscription.filter_parameters)
        signal_memo = memo.get_or_create(key=subscription)
        signal_memo.update(value=book_vwap)
        with super().message_sender.create(type=BookVwapMsg) as book_vwap_msg:
            book_vwap_msg.signal_id = subscription.signal_id
            book_vwap_msg.bid_depth = order_book.bid_depth
            book_vwap_msg.ask_depth = order_book.ask_depth
            book_vwap_msg.vwap = book_vwap
            book_vwap_msg.gradient = signal_memo.gradient() or DEFAULT_GRADIENT
            book_vwap_msg.mid_price = order_book.mid_price
            book_vwap_msg.book_volume = order_book.volume(depth=depth)
            super().message_sender.send(msg=book_vwap_msg)