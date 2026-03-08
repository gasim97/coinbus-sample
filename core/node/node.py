import logging
import traceback

from abc import ABC, abstractmethod
from logging import Logger
from multiprocessing import Process
from multiprocessing.connection import Connection
from typing import Any, Callable, Optional, override

from common.utils.os import pid
from common.utils.pipe import read_pipe_connection_with_readiness_check
from common.utils.pool import ObjectPoolManager
from core.common.readmode import ReadMode
from core.msg.base.msg import Msg
from core.msg.base.msggroup import MsgGroup
from core.msg.msgserializer import MsgSerializer
from core.node.callbackscheduler import CallbackScheduler
from core.node.enginetimeprovider import EngineTimeProvider
from core.node.messagesender import MessageSender
from core.node.subscription import SubscriptionManager
from core.node.resources import Resources
from generated.type.core.enum.environment import EnvironmentEnum
from generated.type.core.msg.activate import ActivateMsg
from generated.type.core.msg.deactivate import DeactivateMsg
from generated.type.core.msg.sessionended import SessionEndedMsg


class Node(ABC, Process):

    __slots__ = (
        '_logger',
        '_name',
        '_environment',
        '_activated',
        '_resources',
        '_engine_read_connection', 
        '_pool_manager',
        '_engine_time_provider', 
        '_callback_scheduler', 
        '_subscription_manager', 
    )

    def __init__(self, name: str, environment: EnvironmentEnum):
        super().__init__()
        self._logger = logging.getLogger(name=name)
        self._name = name
        self._environment = environment
        self._activated = False
        self._resources: Optional[Resources] = None
        self._engine_read_connection: Optional[Connection[Any, Any]] = None
        
        self._pool_manager = ObjectPoolManager()
        self._engine_time_provider = EngineTimeProvider()
        self._callback_scheduler = CallbackScheduler(engine_time_provider=self._engine_time_provider)
        self._subscription_manager = SubscriptionManager(node_name=self._name, callback_handler=self._handle_callback)
    
    @property
    def logger(self) -> Logger:
        return self._logger
    
    @property
    def name(self) -> str:  # type: ignore[override]
        return self._name
    
    @property
    def environment(self) -> EnvironmentEnum:
        return self._environment
    
    @property
    def activated(self) -> bool:
        return self._activated
    
    @property
    def resources(self) -> Resources:
        assert self._resources is not None
        return self._resources
    
    @property
    def pool_manager(self) -> ObjectPoolManager:
        return self._pool_manager
    
    @property
    def engine_time_provider(self) -> EngineTimeProvider:
        return self._engine_time_provider
    
    @property
    def callback_scheduler(self) -> CallbackScheduler:
        return self._callback_scheduler
    
    @property
    def subscription_manager(self) -> SubscriptionManager:
        return self._subscription_manager
    
    @property
    @abstractmethod
    def read_mode(self) -> ReadMode:
        ...
    
    def on_session_started(self) -> None:
        self._create_and_set_resources()
    
    def on_session_ended(self) -> None:
        ...
    
    def _activate(self) -> None:
        self.on_activated()
        self._activated = True
    
    def _deactivate(self) -> None:
        self.on_deactivated()
        self._activated = False
    
    @abstractmethod
    def on_activated(self) -> None:
        ...
    
    @abstractmethod
    def on_deactivated(self) -> None:
        ...
    
    def set_engine_read_connection(self, connection: Connection) -> None:
        self._engine_read_connection = connection
    
    def _create_and_set_resources(self) -> None:
        self._resources = Resources(environment=self.environment)
    
    def run(self) -> None:
        self._logger.info(f"Node started (PID: {pid()})")
        self.on_session_started()
        session_active = True
        while session_active:
            msg: Optional[Msg] = self._read_engine_msg()
            if msg is None:
                continue
            self._handle_start_event_loop_core_utils_callback(msg=msg)
            if msg.group == MsgGroup.CORE:
                if isinstance(msg, ActivateMsg):
                    if msg.node_name == self._name and not self._activated:
                        self._activate()
                        self._logger.info("Node activated")
                if isinstance(msg, DeactivateMsg):
                    if msg.node_name == self._name and self._activated:
                        self._deactivate()
                        self._logger.info("Node deactivated")
                if isinstance(msg, SessionEndedMsg):
                    session_active = False
            self._subscription_manager.handle_msg(msg=msg)
            self._handle_end_event_loop_core_utils_callback(msg=msg)
            msg.release()
        self.on_session_ended()
        self._logger.info("Session ended")
    
    def _read_engine_msg(self) -> Optional[Msg]:
        serialized_msg: Optional[bytes] = read_pipe_connection_with_readiness_check(
            connection=self._engine_read_connection,  # type: ignore[arg-type]
            logger=self._logger, 
            timeout_seconds=self.read_mode.select_timeout_seconds,
        )
        if serialized_msg is None:
            return None
        return MsgSerializer.deserialize(serialized=serialized_msg, pool_manager=self._pool_manager)
    
    def _handle_start_event_loop_core_utils_callback(self, msg: Msg) -> None:
        self._handle_callback(msg=msg, callback=self._engine_time_provider.on_message)
    
    def _handle_end_event_loop_core_utils_callback(self, msg: Msg) -> None:
        self._handle_callback(msg=msg, callback=self._callback_scheduler.on_message)
    
    def _handle_callback(self, msg: Msg, callback: Callable[[Msg], None]) -> None:
        try:
            callback(msg)
        except Exception:
            self._logger.error(f"Uncaught exception handling message: {msg}", exc_info=True)


class ReadOnlyNode(Node):
    
    @override
    @property
    def read_mode(self) -> ReadMode:
        return ReadMode.LONG_BLOCKING


class ReadWriteNode(Node):

    __slots__ = ('_message_sender')

    def __init__(self, name: str, environment: EnvironmentEnum):
        super().__init__(name=name, environment=environment)
        self._message_sender: MessageSender = self._create_message_sender(
            node_name=self.name, 
            logger=self.logger, 
            pool_manager=self.pool_manager, 
        )

    @staticmethod
    def _create_message_sender(
        node_name: str, logger: Logger, pool_manager: ObjectPoolManager,
    ) -> MessageSender:
        return MessageSender(node_name=node_name, logger=logger, pool_manager=pool_manager)
    
    @property
    def message_sender(self) -> MessageSender:
        return self._message_sender
    
    @override
    @property
    def read_mode(self) -> ReadMode:
        return ReadMode.BLOCKING
    
    @override
    def on_session_started(self) -> None:
        super().on_session_started()
        self._message_sender.on_session_started()

    @override
    def _activate(self) -> None:
        self._message_sender.set_activated(activated=True)
        super()._activate()

    @override
    def _deactivate(self) -> None:
        self._message_sender.set_activated(activated=False)
        super()._deactivate()
    
    def set_engine_write_connection(self, connection: Connection) -> None:
        self._message_sender.set_engine_write_connection(connection=connection)
    
    def subscribe_no_messages_in_flight(self, callback: Callable[[], None]) -> None:
        self._message_sender.set_no_messages_in_flight_callback(callback=callback)
    
    @override
    def _handle_start_event_loop_core_utils_callback(self, msg: Msg) -> None:
        self._handle_callback(msg=msg, callback=self._message_sender.on_start_event_loop)
        super()._handle_start_event_loop_core_utils_callback(msg=msg)
    
    @override
    def _handle_end_event_loop_core_utils_callback(self, msg: Msg) -> None:
        super()._handle_end_event_loop_core_utils_callback(msg=msg)
        self._handle_callback(msg=msg, callback=self._message_sender.on_end_event_loop)