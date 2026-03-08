from abc import ABC, abstractmethod
from typing import Generic, Optional, Protocol, Type, TypeVar, override


T = TypeVar('T', covariant=True)


class ObjectInstantiator(Generic[T], Protocol):
    
    @abstractmethod
    def __call__(self) -> T:
        """Instantiate an instance of the object"""
        ...


class _DefaultObjectInstantiator(ObjectInstantiator[T]):

    __slots__ = ('_type')

    def __init__(self, type: Type[T]):
        self._type = type

    @override
    def __call__(self) -> T:
        return self._type()


class ObjectFactory(ABC, Generic[T]):

    DEFAULT_OBJECT_INSTANTIATOR = _DefaultObjectInstantiator

    __slots__ = ('_type', '_object_instantiator')

    def __init__(self, type: Type[T], object_instantiator: Optional[ObjectInstantiator[T]] = None):
        self._type = type
        self._object_instantiator = object_instantiator or self.DEFAULT_OBJECT_INSTANTIATOR(type=self._type)
    
    @property
    def type(self) -> Type[T]:
        return self._type
    
    @property
    def object_instantiator(self) -> ObjectInstantiator[T]:
        return self._object_instantiator
    
    @object_instantiator.setter
    def object_instantiator(self, object_instantiator: ObjectInstantiator[T]) -> None:
        self._object_instantiator = object_instantiator
    
    @abstractmethod
    def get(self) -> T:
        """Get an object instance from the factory"""
        ...


class ObjectFactoryImpl(ObjectFactory[T]):

    def __init__(self, type: Type[T], object_instantiator: Optional[ObjectInstantiator[T]] = None):
        super().__init__(type=type, object_instantiator=object_instantiator)
    
    @override
    def get(self) -> T:
        return self.object_instantiator()