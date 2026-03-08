from abc import ABC, abstractmethod

from dataclasses import dataclass, field
from typing import Any, Generic, Optional, Type, TypeVar, override


class MemoItem(ABC):

    @abstractmethod
    def update(self, **kwargs: Any) -> None:
        ...


K = TypeVar('K')
I = TypeVar('I', bound=MemoItem)
V = TypeVar('V')


@dataclass
class LastValueMemoItem(Generic[V], MemoItem):
    value: Optional[V] = None
    time: Optional[int] = None

    @override
    def update(self, value: Optional[V] = None, time: Optional[int] = None, **kwargs: Any) -> None:
        self.value = value
        self.time = time


@dataclass
class Memo(Generic[K, I]):
    item_type: Type[I]
    memo: dict[K, I] = field(default_factory=dict)

    def get(self, key: K) -> Optional[I]:
        return self.memo.get(key)
    
    def create(self, key: K, **kwargs: Any) -> I:
        self.memo[key] = self.item_type(**kwargs)
        return self.memo[key]
    
    def get_or_create(self, key: K, **kwargs: Any) -> I:
        return self.get(key) or self.create(key, **kwargs)

    def update(self, key: K, **kwargs: Any) -> bool:
        if key not in self.memo:
            return False
        self.memo[key].update(**kwargs)
        return True


def with_memo(key_type: Type[K], item_type: Type[I]):
    memo = Memo[K, I](item_type=item_type)
    def decorator(func):
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs, memo=memo)
        return wrapper
    return decorator