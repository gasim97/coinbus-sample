from typing import override

from core.engine.enricher.enricher import Enricher
from core.msg.base.msg import Msg
from core.msg.base.msggroup import MsgGroup
from generated.type.core.msg.coremsgtype import CoreMsgType
from generated.type.core.msg.request import RequestMsg


class RequestMsgEnricher(Enricher):

    __slots__ = ('_request_id_counter')

    def __init__(self):
        self._request_id_counter = 0

    @classmethod
    @override
    def group(cls) -> MsgGroup:
        return MsgGroup.CORE
    
    @classmethod
    @override
    def type(cls) -> CoreMsgType:
        return CoreMsgType.REQUEST

    @override
    def enrich(self, msg: Msg) -> Msg:
        if not isinstance(msg, RequestMsg):
            return msg
        self._request_id_counter += 1
        msg.request_id = f"{self._request_id_counter}-{msg.context.engine_time}"
        return msg