from typing import override

from application.filelogger.node.textfilelogger import TextFileLoggerNode
from core.msg.base.msg import Msg
from core.msg.base.msggroup import MsgGroup
from generated.type.core.enum.environment import EnvironmentEnum
from generated.type.marketdata.msg.depthupdate import DepthUpdateMsg
from generated.type.marketdata.msg.trade import TradeMsg


class FilteredTextFileLoggerNode(TextFileLoggerNode):

    _IGNORED_MSG_GROUPS: set[MsgGroup] = set([
        MsgGroup.TIMER,
    ])

    _IGNORED_MSGS: set[Msg] = set([
        # DepthUpdateMsg(),
        # TradeMsg(),
    ])

    def __init__(self, name: str = "FILTEREDTEXTFILELOGGER", environment: EnvironmentEnum = EnvironmentEnum.TEST):
        super().__init__(name=name, environment=environment)

    @override
    def handle_msg(self, msg: Msg) -> None:
        if msg.group in self._IGNORED_MSG_GROUPS or msg in self._IGNORED_MSGS:
            return
        super().handle_msg(msg=msg)