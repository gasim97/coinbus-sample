from abc import ABC, abstractmethod

from application.signal.subscription.producermanager import SignalProducerSubscriptionManager
from application.signal.subscription.subscription import SignalSubscription
from application.signal.utils.memo import SignalMemoItem
from common.utils.memo import Memo, with_memo
from core.node.subscription import SubscriptionManager
from core.node.messagesender import MessageSender
from generated.type.signal.enum.signaltype import SignalTypeEnum


class Signal(ABC):

    __slots__ = (
        '_message_sender',
        '_subscription_manager',
        '_signal_subscription_manager',
        '_signal_type',
    )

    def __init__(
        self, 
        message_sender: MessageSender,
        subscription_manager: SubscriptionManager, 
        signal_subscription_manager: SignalProducerSubscriptionManager,
        signal_type: SignalTypeEnum,
    ):
        self._message_sender = message_sender
        self._subscription_manager = subscription_manager
        self._signal_subscription_manager = signal_subscription_manager
        self._signal_type = signal_type
    
    @property
    def message_sender(self) -> MessageSender:
        return self._message_sender
    
    @property
    def subscription_manager(self) -> SubscriptionManager:
        return self._subscription_manager
    
    @property
    def signal_subscription_manager(self) -> SignalProducerSubscriptionManager:
        return self._signal_subscription_manager
    
    @property
    def signal_type(self) -> SignalTypeEnum:
        return self._signal_type

    @abstractmethod
    def subscribe_to_messages(self) -> None:
        ...

    @abstractmethod
    @with_memo(key_type=SignalSubscription, item_type=SignalMemoItem)
    def evaluate(self, subscription: SignalSubscription, memo: Memo[SignalSubscription, SignalMemoItem]) -> None:
        ...