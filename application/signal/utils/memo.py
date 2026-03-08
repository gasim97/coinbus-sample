from dataclasses import dataclass, field
from typing import Any, Optional, override

from common.utils.memo import MemoItem


@dataclass
class SignalMemoItem(MemoItem):
    size: int = 10
    values: list[float] = field(default_factory=list)

    @property
    def value_count(self) -> int:
        return len(self.values)

    @property
    def has_value(self) -> bool:
        return self.value_count > 0

    @property
    def current_value(self) -> Optional[float]:
        return self.values[-1] if self.value_count > 0 else None

    @property
    def current_value_unchecked(self) -> float:
        return self.values[-1]

    def previous_value(self, index: int) -> Optional[float]:
        return self.values[-(index - 1)] if self.value_count > index else None

    def gradient(self, window: Optional[int] = None) -> Optional[float]:
        window = window or self.size - 1
        if self.value_count <= window:
            return None
        return self.values[-1] - self.values[-window - 1]

    @override
    def update(self, value: float = 0.0, **kwargs: Any) -> None:
        if self.value_count >= self.size:
            self.values.pop(0)
        self.values.append(value)

    def clear(self) -> None:
        self.values.clear()