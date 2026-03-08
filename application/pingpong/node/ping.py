from typing import Optional, override

from core.node.node import ReadWriteNode
from generated.type.core.enum.environment import EnvironmentEnum
from generated.type.pingpong.msg.ping import PingMsg
from generated.type.pingpong.msg.pingpongconfiguration import PingPongConfigurationMsg
from generated.type.pingpong.msg.pong import PongMsg


class PingNode(ReadWriteNode):

    __slots__ = ('_game_id')

    def __init__(self, name: str = "PING", environment: EnvironmentEnum = EnvironmentEnum.TEST):
        super().__init__(name=name, environment=environment)
        self._game_id: Optional[int] = None
        self._subscribe()
    
    @override
    def on_activated(self) -> None:
        ...
    
    @override
    def on_deactivated(self) -> None:
        ...

    def _subscribe(self) -> None:
        super().subscription_manager.subscribe_config(type=PingPongConfigurationMsg, callback=self._handle_ping_pong_configuration)
        super().subscription_manager.subscribe(type=PongMsg, callback=self._handle_pong)
    
    def _handle_ping_pong_configuration(self, msg: PingPongConfigurationMsg) -> None:
        self._game_id = msg.game_id
        self._send_ping()
    
    def _handle_pong(self, msg: PongMsg) -> None:
        if msg.game_id == self._game_id:
            self._send_ping()
    
    def _send_ping(self) -> None:
        with super().message_sender.create(type=PingMsg) as ping_msg:
            ping_msg.game_id = self._game_id
            super().message_sender.send(msg=ping_msg)