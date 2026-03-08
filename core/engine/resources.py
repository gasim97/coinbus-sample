from multiprocessing.connection import Connection

from core.engine.nodemanager import EngineNodeManager
from core.msg.base.msg import Msg


class EngineResources:

    __slots__ = ('_admin_read_connection', '_node_managers', '_node_configuration_msgs')

    def __init__(
        self, 
        admin_read_connection: Connection, 
        node_managers: list[EngineNodeManager],
        node_configuration_msgs: dict[str, list[Msg]],
    ):
        self._admin_read_connection = admin_read_connection
        self._node_managers = node_managers
        self._node_configuration_msgs = node_configuration_msgs
    
    @property
    def admin_read_connection(self) -> Connection:
        return self._admin_read_connection
    
    @property
    def node_managers(self) -> list[EngineNodeManager]:
        return self._node_managers
    
    @property
    def node_configuration_msgs(self) -> dict[str, list[Msg]]:
        return self._node_configuration_msgs