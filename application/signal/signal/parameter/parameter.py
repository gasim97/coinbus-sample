from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from generated.type.signal.enum.signalparametertype import SignalParameterTypeEnum


@dataclass
class SignalParameter:
    parameter_type: SignalParameterTypeEnum
    value: Any

    def __hash__(self) -> int:
        return hash((self.parameter_type, self.value))

    def __eq__(self, other) -> bool:
        if not isinstance(other, SignalParameter):
            return False
        return self.parameter_type == other.parameter_type and self.value == other.value