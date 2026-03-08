from typing import Optional, override

from core.node.node import ReadWriteNode
from generated.type.core.enum.environment import EnvironmentEnum
from generated.type.pingpong.msg.ping import PingMsg
from generated.type.pingpong.msg.pingpongconfiguration import PingPongConfigurationMsg
from generated.type.pingpong.msg.pong import PongMsg


class PongNode(ReadWriteNode):

    __slots__ = ('_game_id')

    def __init__(self, name: str = "PONG", environment: EnvironmentEnum = EnvironmentEnum.TEST):
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
        super().subscription_manager.subscribe(type=PingMsg, callback=self._handle_ping)
    
    def _handle_ping_pong_configuration(self, msg: PingPongConfigurationMsg) -> None:
        self._game_id = msg.game_id
    
    def _handle_ping(self, msg: PingMsg) -> None:
        if msg.game_id == self._game_id:
            self._send_pong()
    
    def _send_pong(self) -> None:
        with super().message_sender.create(type=PongMsg) as pong_msg:
            pong_msg.game_id = self._game_id
            super().message_sender.send(msg=pong_msg)