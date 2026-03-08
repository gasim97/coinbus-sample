from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

from generated.type.signal.enum.signalfiltertype import SignalFilterTypeEnum
from generated.type.signal.msg.signalfilter import SignalFilterMsg


@dataclass
class SignalFilterParameter(ABC):
    signal_id: str

    @property
    @abstractmethod
    def filter_type(self) -> SignalFilterTypeEnum:
        ...
    
    @staticmethod
    @abstractmethod
    def from_msg(signal_id: str, msg: SignalFilterMsg) -> SignalFilterParameter:
        ...

    def __hash__(self) -> int:
        return hash((self.signal_id, self.filter_type))

    def __eq__(self, other) -> bool:
        if not isinstance(other, SignalFilterParameter):
            return False
        return self.signal_id == other.signal_id and self.filter_type == other.filter_type