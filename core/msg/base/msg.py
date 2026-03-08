from __future__ import annotations

import numpy as np

from abc import abstractmethod
from copy import deepcopy
from enum import Enum
from json import JSONEncoder, dumps
from typing import Any, Callable, Optional, Self, Type, TypeVar, override

from common.type.wfloat import WFloat
from common.utils.pool import Poolable
from core.msg.base.msggroup import MsgGroup
from core.msg.base.msgtype import MsgType


M = TypeVar("M", bound="Msg")


class MsgDictEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, MsgContext):
            return obj.as_dict
        if isinstance(obj, Msg):
            return obj.as_dict
        if isinstance(obj, Enum):
            return str(obj)
        if isinstance(obj, WFloat):
            return str(obj)
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super().default(obj)


class MsgContext:

    __slots__ = ('_sequence', '_engine_time', '_sender')

    def __init__(self):
        self.clear()
    
    def clear(self) -> Self:
        self._sequence: Optional[int] = None
        self._engine_time: int = 0
        self._sender: Optional[str] = None
        return self
    
    @property
    def is_empty(self) -> bool:
        return self._sequence is None and self._engine_time == 0 and self._sender is None
    
    @property
    def sequence(self) -> Optional[int]:
        return self._sequence

    @sequence.setter
    def sequence(self, sequence: int) -> None:
        self._sequence = sequence

    @property
    def engine_time(self) -> int:
        return self._engine_time

    @engine_time.setter
    def engine_time(self, engine_time: int) -> None:
        self._engine_time = engine_time

    @property
    def sender(self) -> Optional[str]:
        return self._sender

    @sender.setter
    def sender(self, sender: str) -> None:
        self._sender = sender
    
    @property
    def as_dict(self) -> dict[str, Any]:
        return dict(sequence=self._sequence, engine_time=self._engine_time, sender=self._sender)
    
    def from_dict(self, value: Optional[dict[str, Any]]) -> Self:
        if value is None:
            return self.clear()
        self._sequence = value.get("sequence", None)
        self._engine_time = value.get("engine_time", 0)
        self._sender = value.get("sender", None)
        return self

    def copy_from(self, other: MsgContext) -> Self:
        if not isinstance(other, MsgContext):
            raise ValueError(f"Cannot copy msg context from object: {other}")
        self._sequence = other.sequence
        self._engine_time = other.engine_time
        self._sender = other.sender
        return self


class Msg(Poolable):

    __slots__ = ('_context')

    def __init__(self):
        super().__init__()
        self._context = MsgContext()
    
    @override
    def clear(self) -> Self:
        self._context.clear()
        return self
    
    @property
    @abstractmethod
    def group(self) -> MsgGroup:
        ...
    
    @property
    @abstractmethod
    def type(self) -> MsgType:
        ...
    
    @property
    def groups(self) -> list[MsgGroup]:
        return []
    
    @property
    def types(self) -> list[MsgType]:
        return []
    
    @property
    def unique_message_id(self) -> int:
        return self.group.value << 16 | self.type.value

    @property
    def unique_message_ids(self) -> list[int]:
        return [group.value << 16 | type.value for group, type in zip(self.groups, self.types)]
    
    @property
    def context(self) -> MsgContext:
        return self._context
    
    @property
    def as_dict(self) -> dict[str, Any]:
        if not self._context.is_empty:
            return dict(group=self.group, type=self.type, context=self._context)
        return dict(group=self.group, type=self.type)
    
    def from_dict(self, value: dict[str, Any], msg_refs_lookup: Callable[[str, str], Type[Msg]]) -> Self:
        self._context.from_dict(value=value.get("context"))
        return self
    
    def copy_from(self, other: Self) -> Self:
        self.context.copy_from(other=other.context)
        return self

    def clone(self) -> Self:
        return deepcopy(self)

    def create(self, type: Type[M]) -> M:
        if self.object_pool is None or self.object_pool.object_pool_manager is None:
            return type()
        return self.object_pool.object_pool_manager.pool(type=type).get()

    @classmethod
    def release_property(cls, value: Any) -> None:
        if isinstance(value, Poolable):
            value.release()
        elif isinstance(value, list):
            for item in value:
                cls.release_property(item)

    def __deepcopy__(self, memo: dict[int, Any]) -> Msg:
        msg_clone = self.create(type(self))
        msg_clone.copy_from(other=self)
        memo[id(self)] = msg_clone
        return msg_clone
    
    def __str__(self) -> str:
        return dumps(self.as_dict, cls=MsgDictEncoder)
    
    def __repr__(self) -> str:
        return self.__str__()
    
    def __hash__(self) -> int:
        return self.unique_message_id
    
    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Msg):
            return False
        return self.group == other.group and self.type == other.type