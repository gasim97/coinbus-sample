from typing import override

from core.engine.enricher.enricher import Enricher
from core.msg.base.msg import Msg
from core.msg.base.msggroup import MsgGroup
from generated.type.orderentry.msg.orderentrymsgtype import OrderentryMsgType
from generated.type.orderentry.msg.enterorder import EnterOrderMsg


class EnterOrderMsgEnricher(Enricher):

    __slots__ = ('_order_id_counter')

    def __init__(self):
        self._order_id_counter = 0

    @classmethod
    @override
    def group(cls) -> MsgGroup:
        return MsgGroup.ORDERENTRY
    
    @classmethod
    @override
    def type(cls) -> OrderentryMsgType:
        return OrderentryMsgType.ENTERORDER

    @override
    def enrich(self, msg: Msg) -> Msg:
        if not isinstance(msg, EnterOrderMsg):
            return msg
        self._order_id_counter += 1
        msg.internal_order_id = f"{self._order_id_counter}-{msg.context.engine_time}"
        return msg