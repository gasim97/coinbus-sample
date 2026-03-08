from abc import ABC, abstractmethod
from typing import Generic, Optional, Type, TypeVar, override

from common.utils.pipe import SimplexPipe
from core.engine.relay import EngineRelayNode
from core.node.node import Node, ReadWriteNode, ReadOnlyNode
from generated.type.core.enum.environment import EnvironmentEnum


N = TypeVar('N', bound=Node)
RWN = TypeVar('RWN', bound=ReadWriteNode)
RON = TypeVar('RON', bound=ReadOnlyNode)


class EngineNodeManager(Generic[N], ABC):

    __slots__ = ('_name', '_klass', '_engine_to_node_pipe', '_node')

    def __init__(self, name: str, klass: Type[N], engine_to_node_pipe: SimplexPipe):
        super().__init__()
        self._name = name
        self._klass = klass
        self._engine_to_node_pipe = engine_to_node_pipe
        self._node: Optional[N] = None
    
    @property
    def name(self) -> str:
        return self._name
    
    @property
    def engine_to_node_pipe(self) -> SimplexPipe:
        return self._engine_to_node_pipe

    @property
    def node(self) -> N:
        assert self._node is not None, f"Node '{self.name}' has not been initialized"
        return self._node

    def init_node(self, environment: EnvironmentEnum) -> None:
        self._node = self._klass(self._name, environment)
        self.node.set_engine_read_connection(connection=self._engine_to_node_pipe.read_connection)
        self._post_init_node()
    
    @abstractmethod
    def _post_init_node(self) -> None:
        ...
    
    @abstractmethod
    def start_node(self) -> None:
        ...

    @abstractmethod
    def stop_node(self, timeout_seconds: int) -> None:
        ...


class EngineReadWriteNodeManager(EngineNodeManager[RWN]):

    __slots__ = ('_node_to_engine_pipe')

    def __init__(
        self, 
        name: str, 
        klass: Type[RWN], 
        engine_to_node_pipe: SimplexPipe,
        node_to_engine_pipe: SimplexPipe,
    ):
        super().__init__(name=name, klass=klass, engine_to_node_pipe=engine_to_node_pipe)
        self._node_to_engine_pipe = node_to_engine_pipe
    
    @property
    def node_to_engine_pipe(self) -> SimplexPipe:
        return self._node_to_engine_pipe

    @override
    def _post_init_node(self) -> None:
        self.node.set_engine_write_connection(connection=self._node_to_engine_pipe.write_connection)

    @override
    def start_node(self) -> None:
        self.node.start()

    @override
    def stop_node(self, timeout_seconds: int) -> None:
        self.engine_to_node_pipe.write_connection.close()
        self._node_to_engine_pipe.read_connection.close()
        self.node.join(timeout=timeout_seconds)
        self.node.kill()


class EngineReadOnlyNodeManager(EngineNodeManager[RON]):

    __slots__ = ()

    def __init__(self, name: str, klass: Type[RON], engine_to_node_pipe: SimplexPipe):
        super().__init__(name=name, klass=klass, engine_to_node_pipe=engine_to_node_pipe)

    @override
    def _post_init_node(self) -> None:
        ...

    @override
    def start_node(self) -> None:
        self.node.start()

    @override
    def stop_node(self, timeout_seconds: int) -> None:
        self.engine_to_node_pipe.write_connection.close()
        self.node.join(timeout=timeout_seconds)
        self.node.kill()


class EngineRelayNodeManager(EngineReadOnlyNodeManager[EngineRelayNode]):

    __slots__ = ('_relay_to_node_pipes')

    def __init__(
        self, 
        name: str, 
        klass: Type[EngineRelayNode], 
        engine_to_node_pipe: SimplexPipe,
        relay_to_node_pipes: list[SimplexPipe],
    ):
        super().__init__(name=name, klass=klass, engine_to_node_pipe=engine_to_node_pipe)
        self._relay_to_node_pipes = relay_to_node_pipes

    @override
    def _post_init_node(self) -> None:
        super()._post_init_node()
        self.node.set_relay_to_node_pipes(relay_to_node_pipes=self._relay_to_node_pipes)