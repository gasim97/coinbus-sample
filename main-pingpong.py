import logging

from application.filelogger.node.binaryfilelogger import BinaryFileLoggerNode
from application.filelogger.node.textfilelogger import TextFileLoggerNode
from application.pingpong.node.ping import PingNode
from application.pingpong.node.pong import PongNode
from common.utils.time import minutes_to_seconds
from core.engine.launcher import Launcher
from core.node.node import ReadWriteNode
from generated.type.core.enum.environment import EnvironmentEnum
from generated.type.pingpong.msg.pingpongconfiguration import PingPongConfigurationMsg


logging.basicConfig(
  format="%(asctime)s %(levelname)s %(name)-8s %(message)s",
  level=logging.INFO,
  datefmt="%Y-%m-%d %H:%M:%S"
)

def ping_pong_nodes(num_games: int) -> dict[str, ReadWriteNode]:
    ping_nodes = dict((f"PING{i}", PingNode) for i in range(1, num_games + 1))
    pong_nodes = dict((f"PONG{i}", PongNode) for i in range(1, num_games + 1))
    return dict(**ping_nodes, **pong_nodes)

def ping_pong_node_configurations(num_games: int) -> dict[str, list[PingPongConfigurationMsg]]:
    def create_configuration(game_id: int) -> PingPongConfigurationMsg:
        ping_pong_configuration_msg = PingPongConfigurationMsg()
        ping_pong_configuration_msg.game_id = game_id
        return ping_pong_configuration_msg
    
    ping_node_configurations = dict((f"PING{i}", [create_configuration(i)]) for i in range(1, num_games + 1))
    pong_node_configurations = dict((f"PONG{i}", [create_configuration(i)]) for i in range(1, num_games + 1))
    return dict(**ping_node_configurations, **pong_node_configurations)


def ping_pong(num_games: int, run_duration_minutes: float):
    launcher = Launcher(
        environment=EnvironmentEnum.TEST,
        node_klasses=dict(
            TEXTFILELOGGER=TextFileLoggerNode,
            BINARYFILELOGGER=BinaryFileLoggerNode,
            **ping_pong_nodes(num_games=num_games),
        ),
        node_configuration_msgs=ping_pong_node_configurations(num_games=num_games),
    )
    try:
        launcher.start(run_duration_seconds=minutes_to_seconds(run_duration_minutes))
    finally:
        launcher.stop()


if __name__ == "__main__":
   ping_pong(num_games=3, run_duration_minutes=1)