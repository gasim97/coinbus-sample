from dataclasses import dataclass, field
from typing import Any, Generic, Optional, TypeVar, override

from common.utils.memo import MemoItem


V = TypeVar('V', bound=int | float)
G = TypeVar('G', bound=int | float)


@dataclass
class ValueGradientMemoItem(Generic[V, G], MemoItem):
    size: int = 1_000
    values: list[V] = field(default_factory=list)
    gradients: list[G] = field(default_factory=list)

    @property
    def has_value(self) -> bool:
        return len(self.values) > 0

    @property
    def current_value(self) -> Optional[V]:
        return self.values[-1] if len(self.values) > 0 else None

    @property
    def current_gradient(self) -> Optional[G]:
        return self.gradients[-1] if len(self.gradients) > 0 else None
    
    @property
    def previous_gradient(self) -> Optional[G]:
        return self.gradients[-2] if len(self.gradients) > 1 else None

    @property
    def has_gradient_sign_changed(self) -> bool:
        return len(self.gradients) > 1 and self.previous_gradient * self.current_gradient < 0  # type: ignore[operator]
    
    @override
    def update(self, value: V = 0, gradient: G = 0, **kwargs: Any) -> None:  # type: ignore[assignment]
        self.values.append(value)
        self.gradients.append(gradient)
        if len(self.values) > self.size:
            self.values.pop(0)
            self.gradients.pop(0)
