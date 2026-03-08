from abc import ABC, abstractmethod

from application.signal.filter.parameter.parameter import SignalFilterParameter
from generated.type.signal.enum.signalfiltertype import SignalFilterTypeEnum


class SignalFilter(ABC):

    __slots__ = ('_filter_type')

    def __init__(self, filter_type: SignalFilterTypeEnum) -> None:
        self._filter_type = filter_type

    @property
    def filter_type(self) -> SignalFilterTypeEnum:
        return self._filter_type

    @abstractmethod
    def apply(self, value: float, parameter: SignalFilterParameter) -> float:
        ...