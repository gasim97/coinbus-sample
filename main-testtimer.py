import logging

from application.filelogger.node.textfilelogger import TextFileLoggerNode
from application.timer.node.timer import TimerNode
from common.utils.time import minutes_to_seconds
from core.engine.launcher import Launcher
from generated.type.core.enum.environment import EnvironmentEnum


logging.basicConfig(
  format="%(asctime)s %(levelname)s %(name)-8s %(message)s",
  level=logging.INFO,
  datefmt="%Y-%m-%d %H:%M:%S"
)


def test_timer():
    launcher = Launcher(
        environment=EnvironmentEnum.TEST,
        node_klasses=dict(
            TIMER=TimerNode,
            FILELOGGER=TextFileLoggerNode,
        ),
    )
    try:
        launcher.start(run_duration_seconds=minutes_to_seconds(1))
    finally:
        launcher.stop()


if __name__ == "__main__":
   test_timer()