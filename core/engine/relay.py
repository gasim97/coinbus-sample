from abc import abstractmethod
from collections import deque
from typing import override

from common.utils.pipe import (
    SimplexPipe,
    is_connection_ready_for_write,
    safe_write_to_pipe_connection,
)
from core.common.readmode import ReadMode
from core.msg.base.msg import Msg
from core.msg.msgserializer import MsgSerializer
from core.node.node import ReadOnlyNode
from generated.type.core.enum.environment import EnvironmentEnum


class EngineRelayNode(ReadOnlyNode):

    __slots__ = (
        # on set relay to node pipes
        '_relay_to_node_pipes',
        '_nodes_queued_messages',
    )

    def __init__(self, name: str, environment: EnvironmentEnum):
        super().__init__(name=name, environment=environment)
        super().subscription_manager.subscribe_all(callback=self._broadcast_msg)
    
    @override
    @property
    @abstractmethod
    def read_mode(self) -> ReadMode:
        ...
    
    @override
    def on_activated(self) -> None:
        ...
    
    @override
    def on_deactivated(self) -> None:
        ...
    
    @override
    def on_session_ended(self) -> None:
        self._flush()
    
    def set_relay_to_node_pipes(self, relay_to_node_pipes: list[SimplexPipe]) -> None:
        self._relay_to_node_pipes = relay_to_node_pipes
        self._nodes_queued_messages: dict[int, deque[bytes]] = dict(
            (i, deque()) for i in range(len(self._relay_to_node_pipes))
        )
    
    def _broadcast_msg(self, msg: Msg) -> None:
        serialized_msg = MsgSerializer.serialize(msg=msg)
        for i, pipe in enumerate(self._relay_to_node_pipes):
            node_queued_messages = self._nodes_queued_messages[i]
            node_queued_messages.append(serialized_msg)
            while len(node_queued_messages) > 0:
                if not is_connection_ready_for_write(connection=pipe.write_connection):
                    break
                safe_write_to_pipe_connection(
                    connection=pipe.write_connection, 
                    payload=node_queued_messages.popleft(), 
                    logger=self._logger,
                )

    def _flush(self) -> None:
        for i, pipe in enumerate(self._relay_to_node_pipes):
            node_queued_messages = self._nodes_queued_messages[i]
            while len(node_queued_messages) > 0:
                safe_write_to_pipe_connection(
                    connection=pipe.write_connection, 
                    payload=node_queued_messages.popleft(), 
                    logger=self._logger,
                )


class EngineReadOnlyNodeRelay(EngineRelayNode):
    
    @override
    @property
    def read_mode(self) -> ReadMode:
        return ReadMode.BLOCKING


class EngineReadWriteNodeRelay(EngineRelayNode):
    
    @override
    @property
    def read_mode(self) -> ReadMode:
        return ReadMode.BUSY_SPIN