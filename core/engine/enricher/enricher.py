from abc import ABC, abstractmethod

from core.msg.base.msg import Msg
from core.msg.base.msggroup import MsgGroup
from core.msg.base.msgtype import MsgType


class Enricher(ABC):

    @classmethod
    @abstractmethod
    def group(cls) -> MsgGroup:
        ...

    @classmethod
    @abstractmethod
    def type(cls) -> MsgType:
        ...
    
    @property
    def unique_message_id(self) -> int:
        return self.group().value << 16 | self.type().value

    @abstractmethod
    def enrich(self, msg: Msg) -> Msg:
        ...