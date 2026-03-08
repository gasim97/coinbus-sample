from __future__ import annotations

from typing import Generic, Optional, Self, TypeVar, override

from common.utils.pool import DequeObjectPool, Poolable


K = TypeVar('K')
V = TypeVar('V')


class _Node(Poolable, Generic[K, V]):

    __slots__ = ('_key', '_value', '_previous', '_next')

    def __init__(self):
        self._key: Optional[K] = None
        self._value: Optional[V] = None
        self._previous: Optional[_Node[K, V]] = None
        self._next: Optional[_Node[K, V]] = None
    
    def set(self, key: K, value: V) -> Self:
        self._key = key
        self._value = value
        return self

    def update(self, value: V) -> Self:
        self._value = value
        return self
    
    @override
    def clear(self) -> Self:
        self._key = None
        self._value = None
        return self
    
    def link(
        self, 
        previous: Optional[_Node[K, V]] = None,
        next: Optional[_Node[K, V]] = None, 
    ) -> Self:
        self._previous = previous
        self._next = next
        if previous is not None:
            previous._next = self
        if next is not None:
            next._previous = self
        return self
    
    def unlink(self) -> Self:
        if self._previous is not None:
            self._previous._next = self._next
        if self._next is not None:
            self._next._previous = self._previous
        self._previous = None
        self._next = None
        return self
    
    def __str__(self) -> str:
        return f"LRUCache._Node(key={self._key}, value={self._value})"


class LRUCache(Generic[K, V]):

    __slots__ = ('_capacity', '_cache', '_head', '_tail', '_node_pool')

    def __init__(self, capacity: int):
        self._capacity = capacity
        self._cache: dict[K, _Node[K, V]] = {}
        self._head = _Node[K, V]()
        self._tail = _Node[K, V]().link(previous=self._head)
        self._node_pool = DequeObjectPool[_Node[K, V]](
            type=_Node[K, V], 
            expected_size=(capacity + 1),
        )
    
    @property
    def capacity(self) -> int:
        return self._capacity
    
    @property
    def size(self) -> int:
        return len(self._cache)
    
    def contains(self, key: K) -> bool:
        return key in self._cache
    
    def get(self, key: K) -> Optional[V]:
        if key not in self._cache:
            return None
        node = self._cache[key]
        self._add_to_front(node=node.unlink())
        return node._value
    
    def put(self, key: K, value: V) -> V:
        node = (self._cache.get(key) or self._node_pool.get()).unlink().set(key=key, value=value)
        self._cache[key] = node
        self._add_to_front(node=node)
        if self.size > self.capacity:
            self._evict()
        return node._value  # type: ignore
    
    def _add_to_front(self, node: _Node[K, V]) -> None:
        node.link(previous=self._head, next=self._head._next)
    
    def _evict(self) -> None:
        assert self._tail._previous is not None
        node = self._tail._previous.unlink()
        self._cache.pop(node._key)  # type: ignore
        node.release()