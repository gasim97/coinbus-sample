from abc import abstractmethod
from typing import Optional, TypeVar, cast

from application.signal.filter.parameter.mapper import SignalFilterMsgParameterMapper
from application.signal.signal.parameter.mapper import SignalParameterMsgMapper
from application.signal.subscription.subscription import SignalSubscription
from common.type.venuesymbolkey import VenueSymbolKey, VenueSymbolKeyStore
from common.utils.list import none_filter
from common.utils.pool import ObjectPoolManager
from core.node.messagesender import MessageSender
from generated.type.common.enum.symbol import SymbolEnum
from generated.type.common.enum.venue import VenueEnum
from generated.type.signal.enum.signaltype import SignalTypeEnum
from generated.type.signal.msg.signalfilter import SignalFilterMsg
from generated.type.signal.msg.signalmetadata import SignalMetadataMsg
from generated.type.signal.msg.signalparameter import SignalParameterMsg


P = TypeVar('P', bound=SignalParameterMsg)
F = TypeVar('F', bound=SignalFilterMsg)


class SignalProducerSubscriptionManagerListener:

    @abstractmethod
    def on_signal_registered(self, signal_subscription: SignalSubscription) -> None:
        ...


class SignalProducerSubscriptionManager:

    __slots__ = (
        '_node_name',
        '_message_sender', 
        '_listener',
        '_venue_symbol_key_store', 
        '_venue_symbol_signal_subscriptions',
    )

    def __init__(
        self, 
        node_name: str,
        message_sender: MessageSender, 
        listener: SignalProducerSubscriptionManagerListener,
        pool_manager: ObjectPoolManager,
    ):
        self._node_name = node_name
        self._message_sender = message_sender
        self._listener = listener
        self._venue_symbol_key_store = VenueSymbolKeyStore(pool_manager=pool_manager)
        self._venue_symbol_signal_subscriptions: dict[VenueSymbolKey, set[SignalSubscription]] = {}

    def venue_symbol_key(self, venue: VenueEnum, symbol: SymbolEnum) -> VenueSymbolKey:
        return self._venue_symbol_key_store.get(venue=venue, symbol=symbol)
    
    def subscriptions(self, venue: VenueEnum, symbol: SymbolEnum) -> set[SignalSubscription]:
        venue_symbol_key = self.venue_symbol_key(venue=venue, symbol=symbol)
        return self._venue_symbol_signal_subscriptions.get(venue_symbol_key, set())
    
    def register_signal(
        self, 
        request_id: str,
        venue: VenueEnum,
        symbol: SymbolEnum, 
        signal_type: SignalTypeEnum, 
        parameters: list[Optional[P]],
        filters: Optional[list[F]] = None,
        tags: Optional[list[str]] = None,
    ) -> None:
        if venue == VenueEnum.INVALID or symbol == SymbolEnum.INVALID:
            return
            
        valid_parameters = none_filter(values=parameters)
        venue_symbol_key = self.venue_symbol_key(venue=venue, symbol=symbol)
        venue_symbol_subscriptions = self._venue_symbol_signal_subscriptions.setdefault(venue_symbol_key, set())

        signal_id = (
            f"{self._node_name}_{venue}_{symbol}_{signal_type}_"
            f"{hash((
                *[hash((parameter.as_dict.values())) for parameter in valid_parameters], 
                *[hash((filter.as_dict.values())) for filter in filters or []]
            ))}"
        )
        signal_subscription = SignalSubscription(
            signal_id=signal_id,
            venue=venue,
            symbol=symbol,
            signal_type=signal_type, 
            parameters=SignalParameterMsgMapper.map(parameters=valid_parameters),
            filter_parameters=SignalFilterMsgParameterMapper.map(signal_id=signal_id, filters=filters),
            tags=tags,
        )
        if signal_subscription not in venue_symbol_subscriptions:
            venue_symbol_subscriptions.add(signal_subscription)
            self._listener.on_signal_registered(signal_subscription=signal_subscription)
        self._send_signal_metadata(
            request_id=request_id, 
            signal_subscription=signal_subscription, 
            parameters=valid_parameters, 
            filters=filters,
        )
    
    def _send_signal_metadata(
        self, 
        request_id: str,
        signal_subscription: SignalSubscription, 
        parameters: list[P], 
        filters: Optional[list[F]] = None,
    ) -> None:
        with self._message_sender.create(type=SignalMetadataMsg) as signal_metadata_msg:
            signal_metadata_msg.request_id = request_id
            signal_metadata_msg.signal_id = signal_subscription.signal_id
            signal_metadata_msg.venue = signal_subscription.venue
            signal_metadata_msg.symbol = signal_subscription.symbol
            signal_metadata_msg.signal_type = signal_subscription.signal_type
            signal_metadata_msg.parameters = cast(list[SignalParameterMsg], parameters)
            if filters is not None:
                signal_metadata_msg.filters = cast(list[SignalFilterMsg], filters)
            if signal_subscription.tags is not None:
                signal_metadata_msg.tags = signal_subscription.tags
            self._message_sender.send(msg=signal_metadata_msg)