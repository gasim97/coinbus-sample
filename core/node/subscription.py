from typing import Any, Callable, Optional, Type, TypeVar

from core.msg.base.msg import Msg
from generated.type.core.msg.configuration import ConfigurationMsg


M = TypeVar('M', bound=Msg)


class SubscriptionManager:

    __slots__ = (
        '_node_name', 
        '_callback_handler', 
        '_configuration_msg_type_subscriptions',
        '_msg_type_subscriptions',
        '_mine_msg_type_subscriptions',
        '_super_subscription',
    )

    def __init__(self, node_name: str, callback_handler: Callable[[Msg, Callable[[Msg], None]], None]):
        self._node_name = node_name
        self._callback_handler = callback_handler

        self._configuration_msg_type_subscriptions: dict[int, Any] = {}
        self._msg_type_subscriptions: dict[int, Any] = {}
        self._mine_msg_type_subscriptions: dict[int, Any] = {}
        self._super_subscription: Optional[Any] = None
    
    def handle_msg(self, msg: M) -> None:
        if isinstance(msg, ConfigurationMsg):
            config = msg.msg
            if msg.node_name == self._node_name and config is not None:
                for unique_message_id in config.unique_message_ids:
                    if unique_message_id in self._configuration_msg_type_subscriptions:
                        self._callback_handler(config, self._configuration_msg_type_subscriptions[unique_message_id])
                        break
        if self._super_subscription is not None:
            self._callback_handler(msg, self._super_subscription)
            return
        for unique_message_id in msg.unique_message_ids:
            if unique_message_id in self._msg_type_subscriptions:
                self._callback_handler(msg, self._msg_type_subscriptions[unique_message_id])
                return
            elif unique_message_id in self._mine_msg_type_subscriptions:
                if msg.context.sender == self._node_name:
                    self._callback_handler(msg, self._mine_msg_type_subscriptions[unique_message_id])
                    return
    
    def subscribe_config(self, type: Type[M], callback: Callable[[M], None]) -> None:
        instantiated_msg = type()
        if any(unique_message_id in self._configuration_msg_type_subscriptions for unique_message_id in instantiated_msg.unique_message_ids):
            raise RuntimeError(f"Attempt to subscribe multiple times to configuration message type {instantiated_msg.type} in group {instantiated_msg.group}")
        self._configuration_msg_type_subscriptions[instantiated_msg.unique_message_id] = callback
    
    def subscribe(self, type: Type[M], callback: Callable[[M], None]) -> None:
        instantiated_msg = type()
        if (
            any(unique_message_id in self._msg_type_subscriptions for unique_message_id in instantiated_msg.unique_message_ids) 
            or any(unique_message_id in self._mine_msg_type_subscriptions for unique_message_id in instantiated_msg.unique_message_ids) 
            or self._super_subscription is not None
        ):
            raise RuntimeError(f"Attempt to subscribe multiple times to message type {instantiated_msg.type} in group {instantiated_msg.group}")
        self._msg_type_subscriptions[instantiated_msg.unique_message_id] = callback
    
    def subscribe_mine(self, type: Type[M], callback: Callable[[M], None]) -> None:
        instantiated_msg = type()
        if (
            any(unique_message_id in self._mine_msg_type_subscriptions for unique_message_id in instantiated_msg.unique_message_ids) 
            or any(unique_message_id in self._msg_type_subscriptions for unique_message_id in instantiated_msg.unique_message_ids) 
            or self._super_subscription is not None
        ):
            raise RuntimeError(f"Attempt to subscribe multiple times to message type {instantiated_msg.type} in group {instantiated_msg.group}")
        self._mine_msg_type_subscriptions[instantiated_msg.unique_message_id] = callback
    
    def subscribe_all(self, callback: Callable[[M], None]) -> None:
        if (
            self._super_subscription is not None 
            or len(self._mine_msg_type_subscriptions) > 0 
            or len(self._msg_type_subscriptions) > 0
        ):
            raise RuntimeError(f"Attempt to subscribe to all, but already have subscriptions")
        self._super_subscription = callback