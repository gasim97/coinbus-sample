import time

from multiprocessing import Pipe
from typing import Optional, Type, TypeVar

from common.utils.pipe import SimplexPipe
from core.engine.engine import Engine
from core.engine.nodemanager import (
    EngineNodeManager, 
    EngineReadOnlyNodeManager,
    EngineReadWriteNodeManager,
    EngineRelayNodeManager, 
)
from core.engine.relay import EngineReadOnlyNodeRelay, EngineReadWriteNodeRelay, EngineRelayNode
from core.engine.resources import EngineResources
from core.node.node import Node, ReadWriteNode, ReadOnlyNode
from core.msg.base.msg import Msg
from core.msg.msgserializer import MsgSerializer
from generated.type.core.enum.environment import EnvironmentEnum
from generated.type.core.msg.deactivate import DeactivateMsg


ENM = TypeVar("ENM", bound=EngineNodeManager)


class Launcher:

    _READ_WRITE_NODES_RELAY_NAME = "READWRITENODESRELAY"
    _READ_ONLY_NODES_RELAY_NAME = "READONLYNODESRELAY"

    def __init__(
        self, 
        environment: EnvironmentEnum, 
        node_klasses: dict[str, Type[Node]],
        node_configuration_msgs: Optional[dict[str, list[Msg]]] = None,
    ):
        self._read_write_node_managers = []
        self._read_only_node_managers = []
        self._relay_node_managers = []

        read_write_node_klasses = dict(
            (name, klass) for name, klass in node_klasses.items() if issubclass(klass, ReadWriteNode)
        )
        read_only_node_klasses = dict(
            (name, klass) for name, klass in node_klasses.items() if issubclass(klass, ReadOnlyNode)
        )
        if len(read_write_node_klasses) > 0:
            self._read_write_node_managers.extend(
                self._create_read_write_node_managers(read_write_node_klasses=read_write_node_klasses)
            )
            self._relay_node_managers.append(
                self._create_engine_relay_node_manager(
                    name=self._READ_WRITE_NODES_RELAY_NAME, klass=EngineReadWriteNodeRelay, node_managers=self._read_write_node_managers,
                )
            )
        if len(read_only_node_klasses) > 0:
            self._read_only_node_managers.extend(
                self._create_read_only_node_managers(read_only_node_klasses=read_only_node_klasses)
            )
            self._relay_node_managers.append(
                self._create_engine_relay_node_manager(
                    name=self._READ_ONLY_NODES_RELAY_NAME, klass=EngineReadOnlyNodeRelay, node_managers=self._read_only_node_managers,
                )
            )
        
        self._admin_pipe = SimplexPipe(*Pipe(duplex=False))
        self._node_managers: list[EngineNodeManager] = [
            *self._read_write_node_managers, *self._read_only_node_managers, *self._relay_node_managers,
        ]
        self._engine_resources = EngineResources(
            admin_read_connection=self._admin_pipe.read_connection, 
            node_managers=self._node_managers, 
            node_configuration_msgs=node_configuration_msgs or {},
        )
        self._engine = Engine(name="ENGINE", environment=environment, resources=self._engine_resources)
    
    @property
    def nodes(self) -> list[Node]:
        return self._engine.nodes
    
    # FIXME: catch user interrupt and send deactivate message then wait for processes to stop
    def start(self, run_duration_seconds: Optional[int] = None) -> None:
        self._engine.start()
        if run_duration_seconds is None:
            self._engine.join()
            return
        time.sleep(run_duration_seconds)
    
    def stop(self) -> None:
        deactivate_msg = DeactivateMsg()
        deactivate_msg.node_name = self._engine.name
        self._admin_pipe.write_connection.send_bytes(buf=MsgSerializer.serialize(msg=deactivate_msg))
    
    @staticmethod
    def _create_read_write_node_managers(
        read_write_node_klasses: dict[str, Type[ReadWriteNode]]
    ) -> list[EngineReadWriteNodeManager]:
        return [
            EngineReadWriteNodeManager(
                name=name, 
                klass=klass, 
                engine_to_node_pipe=SimplexPipe(*Pipe(duplex=False)), 
                node_to_engine_pipe=SimplexPipe(*Pipe(duplex=False)),
            ) 
            for name, klass in read_write_node_klasses.items()
        ]
    
    @staticmethod
    def _create_read_only_node_managers(
        read_only_node_klasses: dict[str, Type[ReadOnlyNode]],
    ) -> list[EngineReadOnlyNodeManager]:
        return [
            EngineReadOnlyNodeManager(name=name, klass=klass, engine_to_node_pipe=SimplexPipe(*Pipe(duplex=False)))
            for name, klass in read_only_node_klasses.items()
        ]
    
    @staticmethod
    def _create_engine_relay_node_manager(
        name: str, klass: Type[EngineRelayNode], node_managers: list[ENM],
    ) -> EngineRelayNodeManager:
        return EngineRelayNodeManager(
            name=name, 
            klass=klass, 
            engine_to_node_pipe=SimplexPipe(*Pipe(duplex=False)), 
            relay_to_node_pipes=[node_manager.engine_to_node_pipe for node_manager in node_managers],
        )
