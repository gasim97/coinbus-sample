import logging
import time

from multiprocessing import Process
from multiprocessing.connection import Connection
from typing import Optional

from common.utils.os import pid
from common.utils.pipe import (
    connections_ready_for_read, 
    read_pipe_connection, 
    read_pipe_connection_with_readiness_check, 
    safe_write_to_pipe_connection,
)
from common.utils.pool import ObjectPoolManager
from core.common.readmode import ReadMode
from core.engine.enricher.enricher import Enricher
from core.engine.enricher.enterorder import EnterOrderMsgEnricher
from core.engine.enricher.request import RequestMsgEnricher
from core.engine.nodemanager import EngineReadWriteNodeManager, EngineRelayNodeManager
from core.engine.resources import EngineResources
from core.node.node import Node
from core.msg.base.msg import Msg
from core.msg.msgserializer import MsgSerializer
from generated.type.core.enum.environment import EnvironmentEnum
from generated.type.core.msg.activate import ActivateMsg
from generated.type.core.msg.configuration import ConfigurationMsg
from generated.type.core.msg.deactivate import DeactivateMsg
from generated.type.core.msg.sessionended import SessionEndedMsg


class Engine(Process):

    _READ_MODE = ReadMode.BUSY_SPIN
    _STOP_NODE_TIMEOUT_SECONDS = 10

    _ENRICHERS: list[Enricher] = [
        EnterOrderMsgEnricher(),
        RequestMsgEnricher(),
    ]
    _ENRICHER_BY_ID: dict[int, Enricher] = dict((enricher.unique_message_id, enricher) for enricher in _ENRICHERS)

    __slots__ = (
        '_logger',
        '_name',
        '_environment',
        '_resources',
        '_engine_to_relay_pipes',
        '_node_to_engine_read_connections',
        '_node_name_by_node_to_engine_read_connections',
        '_pool_manager',
        '_activated',
        '_sequence',
    )

    def __init__(self, name: str, environment: EnvironmentEnum, resources: EngineResources):
        super().__init__()
        self._logger = logging.getLogger(name=name)
        self._name = name
        self._environment = environment
        self._resources = resources
        self._engine_to_relay_pipes = [
            manager.engine_to_node_pipe for manager in self._resources.node_managers 
            if isinstance(manager, EngineRelayNodeManager)
        ]
        self._node_to_engine_read_connections = [
            manager.node_to_engine_pipe.read_connection for manager in self._resources.node_managers 
            if isinstance(manager, EngineReadWriteNodeManager)
        ]
        self._node_name_by_node_to_engine_read_connection = dict(
            (manager.node_to_engine_pipe.read_connection, manager.name) 
            for manager in self._resources.node_managers 
            if isinstance(manager, EngineReadWriteNodeManager)
        )
        self._pool_manager = ObjectPoolManager()
        self._activated = False
        self._sequence = 0
    
    @property
    def name(self) -> str:  # type: ignore[override]
        return self._name
    
    @property
    def environment(self) -> EnvironmentEnum:
        return self._environment
    
    @property
    def nodes(self) -> list[Node]:
         return [manager.node for manager in self._resources.node_managers]

    @property
    def activated(self) -> bool:
        return self._activated

    def _activate(self) -> None:
        self._activated = True

    def _deactivate(self) -> None:
        self._activated = False
    
    def run(self) -> None:
        self._logger.info(f"Engine started (PID: {pid()})")
        self._init_nodes()
        self._start_nodes()
        self._activate()
        self._send_node_configuration_msgs()
        while self.activated:
            self._read_node_connections()
            self._read_admin_connection()
        self._stop_nodes()

    def _init_nodes(self) -> None:
        for manager in self._resources.node_managers:
            manager.init_node(environment=self.environment)
        node_names = [node.name for node in self.nodes]
        if self.name in node_names:
            raise ValueError(f"Engine name '{self.name}' clashes with a node name in the provided nodes list: {node_names}")
        if len(node_names) != len(set(node_names)):
            raise ValueError(f"There are node name clashes in the provided nodes list: {node_names}")

    def _start_nodes(self) -> None:
        for manager in self._resources.node_managers:
            manager.start_node()
        with self._pool_manager.pool(type=ActivateMsg).get() as activate_msg:
            for node in self.nodes:
                activate_msg.node_name = node.name
                self._broadcast_msg(msg=activate_msg, sender=self.name)
        
    def _stop_nodes(self) -> None:
        with self._pool_manager.pool(type=DeactivateMsg).get() as deactivate_msg:
            for node in self.nodes:
                deactivate_msg.node_name = node.name
                self._broadcast_msg(msg=deactivate_msg, sender=self.name)
        with self._pool_manager.pool(type=SessionEndedMsg).get() as session_ended:
            self._broadcast_msg(msg=session_ended, sender=self.name)
        for manager in self._resources.node_managers:
            manager.stop_node(timeout_seconds=self._STOP_NODE_TIMEOUT_SECONDS)
    
    def _send_node_configuration_msgs(self) -> None:
        with self._pool_manager.pool(type=ConfigurationMsg).get() as configuration_msg:
            for node_name, msgs in self._resources.node_configuration_msgs.items():
                for msg in msgs:
                    configuration_msg.node_name = node_name
                    configuration_msg.msg = msg
                    self._broadcast_msg(msg=configuration_msg, sender=self.name)

    def _read_admin_connection(self) -> None:
        serialized_admin_msg: Optional[bytes] = read_pipe_connection_with_readiness_check(
            connection=self._resources.admin_read_connection, logger=self._logger,
        )
        if serialized_admin_msg is not None:
            admin_msg = MsgSerializer.deserialize(serialized=serialized_admin_msg, pool_manager=self._pool_manager)
            if admin_msg is None:
                self._logger.error(f"Failed to deserialize message from admin: {serialized_admin_msg!r}")
            else:
                self._broadcast_msg(msg=admin_msg, sender=self.name)
                if isinstance(admin_msg, DeactivateMsg) and admin_msg.node_name == self.name:
                    self._deactivate()
                admin_msg.release()

    def _read_node_connections(self) -> None:
        node_connections_ready_for_read: list[Connection] = connections_ready_for_read(
            connections=self._node_to_engine_read_connections, timeout_seconds=self._READ_MODE.select_timeout_seconds,
        )
        for connection in node_connections_ready_for_read:
            serialized_msg: Optional[bytes] = read_pipe_connection(
                connection=connection, logger=self._logger,
            )
            if serialized_msg is None:
                continue
            msg = MsgSerializer.deserialize(serialized=serialized_msg, pool_manager=self._pool_manager)
            if msg is None:
                self._logger.error(f"Failed to deserialize message from node "
                                    f"{self._node_name_by_node_to_engine_read_connection[connection]}: "
                                    f"{serialized_msg!r}")
                continue
            self._broadcast_msg(msg=msg, sender=self._node_name_by_node_to_engine_read_connection[connection])
            msg.release()
        
    def _broadcast_msg(self, msg: Msg, sender: str) -> None:
        msg = self._set_msg_context(msg=msg, sender=sender)
        for unique_message_id in msg.unique_message_ids:
            enricher = self._ENRICHER_BY_ID.get(unique_message_id)
            if enricher is not None:
                msg = enricher.enrich(msg=msg)
        serialized_msg = MsgSerializer.serialize(msg=msg)
        for pipe in self._engine_to_relay_pipes:
            safe_write_to_pipe_connection(
                connection=pipe.write_connection, 
                payload=serialized_msg, 
                logger=self._logger,
            )
    
    def _set_msg_context(self, msg: Msg, sender: str) -> Msg:
        self._sequence += 1
        msg.context.engine_time = time.time_ns()
        msg.context.sequence = self._sequence
        msg.context.sender = sender
        return msg