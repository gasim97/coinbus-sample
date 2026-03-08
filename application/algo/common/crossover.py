from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, unique
from typing import Any, Optional, override

from common.utils.memo import Memo, MemoItem, with_memo


@dataclass
class _SignalCrossoverMemoItem(MemoItem):
    signal_1_value: Optional[float] = None
    signal_2_value: Optional[float] = None

    @property
    def has_values(self) -> bool:
        return self.signal_1_value is not None and self.signal_2_value is not None

    @override
    def update(self, signal_1_value: float = 0.0, signal_2_value: Optional[float] = None, **kwargs: Any) -> None:
        self.signal_1_value = signal_1_value
        self.signal_2_value = signal_2_value


@unique
class CrossoverDirection(int, Enum):
    NONE = 0
    UP = 1
    DOWN = 2

    def inverted(self) -> CrossoverDirection:
        if self == CrossoverDirection.UP:
            return CrossoverDirection.DOWN
        elif self == CrossoverDirection.DOWN:
            return CrossoverDirection.UP
        else:
            return CrossoverDirection.NONE

    @staticmethod
    @with_memo(key_type=str, item_type=_SignalCrossoverMemoItem)
    def detect(
        signal_1_value: Optional[float], 
        signal_2_value: Optional[float], 
        crossover_id: str, 
        memo: Memo[str, _SignalCrossoverMemoItem],
    ) -> CrossoverDirection:
        if signal_1_value is None or signal_2_value is None:
            return CrossoverDirection.NONE

        memo_item = memo.get_or_create(key=crossover_id, signal_1_value=signal_1_value, signal_2_value=signal_2_value)
        direction = CrossoverDirection.NONE
        if memo_item.has_values:
            if signal_1_value >= signal_2_value and memo_item.signal_1_value < memo_item.signal_2_value:  # type: ignore[operator]
                direction = CrossoverDirection.UP
            elif signal_1_value < signal_2_value and memo_item.signal_1_value >= memo_item.signal_2_value:  # type: ignore[operator]
                direction = CrossoverDirection.DOWN
        memo_item.update(signal_1_value=signal_1_value, signal_2_value=signal_2_value)
        return direction