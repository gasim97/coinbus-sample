from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Optional, Self, Type, TypeVar, override
from collections import deque

from common.type.linkedlist import LinkedList
from common.utils.factory import ObjectFactory, ObjectInstantiator


T = TypeVar('T')


class Poolable(ABC):

    __slots__ = ('_object_pool')

    def __init__(self, object_pool: Optional[ObjectPool[Self]] = None):
        self._object_pool: Optional[ObjectPool[Self]] = object_pool
    
    @property
    def object_pool(self) -> Optional[ObjectPool[Self]]:
        return self._object_pool
    
    @object_pool.setter
    def object_pool(self, object_pool: ObjectPool[Self]) -> None:
        self._object_pool = object_pool

    @abstractmethod
    def clear(self) -> Self:
        ...

    def release(self) -> None:
        if self._object_pool is not None:
            self._object_pool.release(object=self)  # type: ignore

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.release()


class _ObjectPoolInstantiator(ObjectInstantiator[T]):

    __slots__ = ('_object_pool', '_object_instantiator')

    def __init__(self, object_pool: ObjectPool[T], object_instantiator: ObjectInstantiator[T]):
        self._object_pool = object_pool
        self._object_instantiator = object_instantiator

    @override
    def __call__(self) -> T:
        object = self._object_instantiator()
        if isinstance(object, Poolable):
            object.object_pool = self._object_pool  # type: ignore
        return object


class ObjectPool(ObjectFactory[T]):

    __slots__ = ('_object_pool_manager')

    def __init__(self, type: Type[T], object_instantiator: Optional[ObjectInstantiator[T]] = None):
        super().__init__(
            type=type, 
            object_instantiator=_ObjectPoolInstantiator(
                object_pool=self, 
                object_instantiator=object_instantiator or ObjectFactory.DEFAULT_OBJECT_INSTANTIATOR(type=type),
            )
        )
        self._object_pool_manager: Optional[ObjectPoolManager] = None
    
    @override
    @property
    def object_instantiator(self) -> ObjectInstantiator[T]:
        return self._object_instantiator
    
    @object_instantiator.setter
    @override
    def object_instantiator(self, object_instantiator: ObjectInstantiator[T]) -> None:
        self._object_instantiator = _ObjectPoolInstantiator(object_pool=self, object_instantiator=object_instantiator)
    
    @property
    def object_pool_manager(self) -> Optional[ObjectPoolManager]:
        return self._object_pool_manager
    
    @object_pool_manager.setter
    def object_pool_manager(self, object_pool_manager: ObjectPoolManager) -> None:
        self._object_pool_manager = object_pool_manager
    
    @abstractmethod
    @override
    def get(self) -> T:
        """Get an object instance from the pool"""
        ...
    
    @abstractmethod
    def release(self, object: T) -> None:
        """Release an object instance back to the pool"""
        ...
    
    @abstractmethod
    def add_expected_size(self, expected_size: Optional[int]) -> Self:
        """Increase the pool size by an additional expected size"""
        ...


class ObjectPoolManager:

    _DEFAULT_POOL_SIZE = 16

    __slots__ = ('_global_object_pools', '_local_object_pools')

    def __init__(self):
        self._global_object_pools: dict[Type[Any], ObjectPool[Any]] = {}
        self._local_object_pools: list[ObjectPool[Any]] = []
    
    def pool(self, type: Type[T], object_instantiator: Optional[ObjectInstantiator[T]] = None) -> ObjectPool[T]:
        return self._global_object_pools.get(type) or self._create_global_pool(type=type, object_instantiator=object_instantiator)
    
    def pre_sized_pool(self, type: Type[T], expected_size: int, object_instantiator: Optional[ObjectInstantiator[T]] = None) -> ObjectPool[T]:
        pool = self._global_object_pools.get(type)
        if pool is not None:
            pool.add_expected_size(expected_size=expected_size)
            return pool
        return self._create_global_pool(type=type, expected_size=expected_size, object_instantiator=object_instantiator)
    
    def create_local_pool(self, type: Type[T], expected_size: Optional[int] = None, object_instantiator: Optional[ObjectInstantiator[T]] = None) -> ObjectPool[T]:
        pool = self._create_pool(type=type, expected_size=expected_size, object_instantiator=object_instantiator)
        self._local_object_pools.append(pool)
        return pool
    
    def _create_global_pool(self, type: Type[T], expected_size: Optional[int] = None, object_instantiator: Optional[ObjectInstantiator[T]] = None) -> ObjectPool[T]:
        pool = self._create_pool(type=type, expected_size=expected_size, object_instantiator=object_instantiator)
        self._global_object_pools[type] = pool
        return pool
    
    def _create_pool(self, type: Type[T], expected_size: Optional[int] = None, object_instantiator: Optional[ObjectInstantiator[T]] = None) -> ObjectPool[T]:
        expected_size = expected_size or self._DEFAULT_POOL_SIZE
        pool = DequeObjectPool[T](type=type, object_instantiator=object_instantiator, expected_size=expected_size)
        pool.object_pool_manager = self
        return pool


class ListObjectPool(ObjectPool[T]):
    """Object pool implementation using list as the underlying storage"""

    __slots__ = ('_pool')

    def __init__(
        self, 
        type: Type[T], 
        object_instantiator: Optional[ObjectInstantiator[T]] = None, 
        expected_size: Optional[int] = None, 
    ):
        super().__init__(type=type, object_instantiator=object_instantiator)
        self._pool: list[T] = []
        self.add_expected_size(expected_size=expected_size)
    
    @override
    def get(self) -> T:
        return self._pool.pop() if len(self._pool) > 0 else self.object_instantiator()
    
    @override
    def release(self, object: T) -> None:
        if isinstance(object, Poolable):
            object.clear()
        self._pool.append(object)
    
    @override
    def add_expected_size(self, expected_size: Optional[int]) -> Self:
        if expected_size is not None:
            for _ in range(expected_size):
                self._pool.append(self.object_instantiator())
        return self


class LinkedListObjectPool(ObjectPool[T]):
    """Object pool implementation using LinkedList as the underlying storage"""

    __slots__ = ('_pool')

    def __init__(
        self, 
        type: Type[T], 
        object_instantiator: Optional[ObjectInstantiator[T]] = None, 
        expected_size: Optional[int] = None, 
    ):
        super().__init__(type=type, object_instantiator=object_instantiator)
        self._pool = LinkedList[T](expected_size=expected_size)
        self.add_expected_size(expected_size=expected_size)
    
    @override
    def get(self) -> T:
        return self._pool.head or self.object_instantiator()
    
    @override
    def release(self, object: T) -> None:
        if isinstance(object, Poolable):
            object.clear()
        self._pool.head = object
    
    @override
    def add_expected_size(self, expected_size: Optional[int]) -> Self:
        if expected_size is not None:
            for _ in range(expected_size):
                self._pool.head = self.object_instantiator()
        return self


class DequeObjectPool(ObjectPool[T]):
    """Object pool implementation using collections.deque as the underlying storage"""

    __slots__ = ('_pool')

    def __init__(
        self, 
        type: Type[T], 
        object_instantiator: Optional[ObjectInstantiator[T]] = None, 
        expected_size: Optional[int] = None, 
    ):
        super().__init__(type=type, object_instantiator=object_instantiator)
        self._pool = deque[T]()
        self.add_expected_size(expected_size=expected_size)
    
    @override
    def get(self) -> T:
        try:
            return self._pool.popleft()
        except IndexError:
            return self.object_instantiator()
    
    @override
    def release(self, object: T) -> None:
        if isinstance(object, Poolable):
            object.clear()
        self._pool.append(object)
    
    @override
    def add_expected_size(self, expected_size: Optional[int]) -> Self:
        if expected_size is not None:
            for _ in range(expected_size):
                self._pool.append(self.object_instantiator())
        return self