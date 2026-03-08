from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, unique
from math import ceil, floor
from typing import Callable, Optional

from common.utils.number import decimal_places_from_string


@dataclass
class _Operation:
    value: Callable[[int, int], int | float]
    exponent: Callable[[int], int]

    def eval(
        self,
        x: float, 
        y: float, 
        precision: int, 
    ) -> WFloat:
        scaling_factor = 10 ** precision
        descaling_factor = 10 ** self.exponent(precision)
        scaled_value = self.value(int(x * scaling_factor), int(y * scaling_factor))
        return WFloat(value=scaled_value / descaling_factor, precision=precision)


@unique
class _OperationType(Enum):
    ADD = _Operation(value=lambda x, y: x + y, exponent=lambda x: x)
    SUB = _Operation(value=lambda x, y: x - y, exponent=lambda x: x)
    MUL = _Operation(value=lambda x, y: x * y, exponent=lambda x: x + x)
    DIV = _Operation(value=lambda x, y: x / y, exponent=lambda x: x - x)
    FLOORDIV = _Operation(value=lambda x, y: x // y, exponent=lambda x: x - x)
    MOD = _Operation(value=lambda x, y: x % y, exponent=lambda x: x)
    POW = _Operation(value=lambda x, y: x ** y, exponent=lambda x: x * x)


class WFloat:

    __slots__ = ('_value', '_precision', '_scaling_factor', '_float', '_value_int', '_value_str')

    def __init__(self, value: float, precision: int):
        self._value = value
        self._precision = precision
        self._scaling_factor = 10 ** self._precision
        self._float = ((self._value * self._scaling_factor) // 1) / self._scaling_factor
        self._value_int: Optional[int] = None
        self._value_str: Optional[str] = None

    @property
    def precision(self) -> int:
        return self._precision

    @property
    def value(self) -> float:
        return self._value

    @property
    def as_float(self) -> float:
        return self._float

    @property
    def as_int(self) -> int:
        if self._value_int is None:
            self._value_int = int(self._value)
        return self._value_int

    @property
    def as_str(self) -> str:
        if self._value_str is None:
            self._value_str = format(self._value, f'.{self._precision}f')
        return self._value_str

    def clone(self) -> WFloat:
        return WFloat(value=self._value, precision=self._precision)

    def __str__(self) -> str:
        return self.as_str

    def __hash__(self) -> int:
        return hash((self.as_float, self._precision))

    def __cmp__(self, other: WFloat | int | float) -> int:
        if isinstance(other, WFloat):
            return -1 if self.as_float < other.as_float else 0 if self.as_float == other.as_float else 1
        return -1 if self.as_float < other else 0 if self.as_float == other else 1

    def __eq__(self, other: object, /) -> bool:
        if isinstance(other, WFloat):
            return self.as_float == other.as_float
        return self.as_float == other

    def __ne__(self, other: object, /) -> bool:
        return not self.__eq__(other)

    def __gt__(self, other: WFloat | int | float) -> bool:
        if isinstance(other, WFloat):
            return self.as_float > other.as_float
        return self.as_float > other

    def __lt__(self, other: WFloat | int | float) -> bool:
        if isinstance(other, WFloat):
            return self.as_float < other.as_float
        return self.as_float < other

    def __ge__(self, other: WFloat | int | float) -> bool:
        if isinstance(other, WFloat):
            return self.as_float >= other.as_float
        return self.as_float >= other

    def __le__(self, other: WFloat | int | float) -> bool:
        if isinstance(other, WFloat):
            return self.as_float <= other.as_float
        return self.as_float <= other

    def __abs__(self) -> WFloat:
        return WFloat(value=abs(self.as_float), precision=self._precision)

    def __add__(self, other: WFloat | int | float) -> WFloat:
        return self._op(wfloat=self, other=other, operation_type=_OperationType.ADD)

    def __sub__(self, other: WFloat | int | float) -> WFloat:
        return self._op(wfloat=self, other=other, operation_type=_OperationType.SUB)

    def __mul__(self, other: WFloat | int | float) -> WFloat:
        return self._op(wfloat=self, other=other, operation_type=_OperationType.MUL)

    def __div__(self, other: WFloat | int | float) -> WFloat:
        return self._op(wfloat=self, other=other, operation_type=_OperationType.DIV)

    def __truediv__(self, other: WFloat | int | float) -> WFloat:
        return self._op(wfloat=self, other=other, operation_type=_OperationType.DIV)

    def __floordiv__(self, other: WFloat | int | float) -> WFloat:
        return self._op(wfloat=self, other=other, operation_type=_OperationType.FLOORDIV)

    def __mod__(self, other: WFloat | int | float) -> WFloat:
        return self._op(wfloat=self, other=other, operation_type=_OperationType.MOD)

    def __pow__(self, other: WFloat | int | float) -> WFloat:
        return self._op(wfloat=self, other=other, operation_type=_OperationType.POW)

    @staticmethod
    def _op(
        wfloat: WFloat,
        other: WFloat | int | float,
        operation_type: _OperationType,
    ) -> WFloat:
        if isinstance(other, WFloat):
            return operation_type.value.eval(x=wfloat.as_float, y=other.as_float, precision=max(wfloat._precision, other.precision))
        return operation_type.value.eval(x=wfloat.as_float, y=other, precision=wfloat._precision)

    @staticmethod
    def round(value: float, precision: int) -> WFloat:
        scaling_factor = 10 ** precision
        result = round(value * scaling_factor) / scaling_factor
        return WFloat(value=result, precision=precision)

    @staticmethod
    def round_up(value: float, precision: int) -> WFloat:
        scaling_factor = 10 ** precision
        result = ceil(value * scaling_factor) / scaling_factor
        return WFloat(value=result, precision=precision)

    @staticmethod
    def round_down(value: float, precision: int) -> WFloat:
        scaling_factor = 10 ** precision
        result = floor(value * scaling_factor) / scaling_factor
        return WFloat(value=result, precision=precision)

    @staticmethod
    def from_string(value: str) -> WFloat:
        return WFloat(value=float(value), precision=decimal_places_from_string(value=value, trim=False))