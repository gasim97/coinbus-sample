from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Type, TypeVar

from application.signal.filter.parameter.parameter import SignalFilterParameter
from application.signal.signal.parameter.parameter import SignalParameter
from generated.type.common.enum.symbol import SymbolEnum
from generated.type.common.enum.venue import VenueEnum
from generated.type.signal.enum.signalparametertype import SignalParameterTypeEnum
from generated.type.signal.enum.signaltype import SignalTypeEnum


T = TypeVar('T')


@dataclass
class SignalSubscription:
    signal_id: str
    venue: VenueEnum
    symbol: SymbolEnum
    signal_type: SignalTypeEnum
    parameters: list[SignalParameter]
    filter_parameters: list[SignalFilterParameter]
    tags: Optional[list[str]]

    def parameter(self, parameter_type: SignalParameterTypeEnum, type: Type[T]) -> Optional[T]:
        for parameter in self.parameters:
            if parameter.parameter_type == parameter_type and isinstance(parameter.value, type):
                return parameter.value
        return None
    
    def has_tag(self, tag: str) -> bool:
        if self.tags is None:
            return False
        return tag in self.tags
    
    def has_any_tag(self, tags: list[str]) -> bool:
        if self.tags is None:
            return False
        return any(tag in self.tags for tag in tags)
    
    def has_all_tags(self, tags: list[str]) -> bool:
        if self.tags is None:
            return False
        return all(tag in self.tags for tag in tags)

    def __hash__(self) -> int:
        return hash(self.signal_id)

    def __eq__(self, other) -> bool:
        if not isinstance(other, SignalSubscription):
            return False
        return self.signal_id == other.signal_id