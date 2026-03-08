from dataclasses import dataclass
from typing import Any, Callable, Generic, Optional, Type, TypeVar

from application.signal.filter.parameter.mapper import SignalFilterMsgParameterMapper
from application.signal.signal.parameter.mapper import SignalParameterMsgMapper
from application.signal.subscription.subscription import SignalSubscription
from core.node.subscription import SubscriptionManager
from generated.type.signal.msg.signalmetadata import SignalMetadataMsg
from generated.type.signal.msg.signalvalue import SignalValueMsg


T = TypeVar("T", bound=SignalValueMsg)


@dataclass
class SignalValueCallback(Generic[T]):
    type: Type[T]
    callback: Callable[[T, SignalSubscription], None]


@dataclass
class _SignalStoreSubscription(SignalSubscription, Generic[T]):
    signal_value_callback: SignalValueCallback[T]


class SignalConsumerSubscriptionManager:

    __slots__ = ("_signal_value_callback_by_request_id", "_signal_subscription_by_signal_id")

    def __init__(self, subscription_manager: SubscriptionManager):
        self._signal_value_callback_by_request_id: dict[str, SignalValueCallback[Any]] = {}
        self._signal_subscription_by_signal_id: dict[str, _SignalStoreSubscription[Any]] = {}
        self._subscribe_to_messages(subscription_manager=subscription_manager)

    def _subscribe_to_messages(self, subscription_manager: SubscriptionManager) -> None:
        subscription_manager.subscribe(type=SignalMetadataMsg, callback=self._handle_signal_metadata)
        subscription_manager.subscribe(type=SignalValueMsg, callback=self._handle_signal_value)

    def register(self, request_id: str, type: Type[T], callback: Callable[[T, SignalSubscription], None]) -> None:
        self._signal_value_callback_by_request_id[request_id] = SignalValueCallback[T](type=type, callback=callback)

    def subscription(self, signal_id: str) -> Optional[SignalSubscription]:
        return self._signal_subscription_by_signal_id.get(signal_id)
        
    def _handle_signal_metadata(self, msg: SignalMetadataMsg) -> None:
        signal_callback = self._signal_value_callback_by_request_id.pop(msg.request_id, None)
        if signal_callback is None:
            return
        signal_subscription = _SignalStoreSubscription[SignalValueMsg](
            signal_id=msg.signal_id,
            venue=msg.venue,
            symbol=msg.symbol,
            signal_type=msg.signal_type,
            parameters=SignalParameterMsgMapper.map(parameters=msg.parameters),
            filter_parameters=SignalFilterMsgParameterMapper.map(signal_id=msg.signal_id, filters=msg.filters),
            tags=msg.tags,
            signal_value_callback=signal_callback,
        )
        self._signal_subscription_by_signal_id[signal_subscription.signal_id] = signal_subscription
    
    def _handle_signal_value(self, msg: SignalValueMsg) -> None:
        signal_subscription = self._signal_subscription_by_signal_id.get(msg.signal_id)
        if signal_subscription is not None and isinstance(msg, signal_subscription.signal_value_callback.type):
            signal_subscription.signal_value_callback.callback(msg, signal_subscription)