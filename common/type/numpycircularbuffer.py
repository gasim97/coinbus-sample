from __future__ import annotations

import numpy as np

from typing import Any, Iterable, Iterator, MutableSequence, Optional, TypeVar, overload, override


T = TypeVar('T', bound=int | float)


class NumpyCircularBuffer(MutableSequence[T]):

    __slots__ = ('_max_length', '_data', '_head', '_size', '_data_type')

    def __init__(self, max_length: int, data_type: type[T]) -> None:
        if max_length <= 0:
            raise ValueError("max_length must be positive")
        self._max_length = max_length
        self._data_type = data_type
        self._data = np.empty(shape=max_length, dtype=data_type)
        self._head = 0
        self._size = 0

    def __len__(self) -> int:
        return self._size

    @overload
    def __getitem__(self, index: int, /) -> T: ...

    @overload
    def __getitem__(self, index: slice, /) -> MutableSequence[T]: ...

    def __getitem__(self, index: int | slice, /) -> T | MutableSequence[T]:
        if isinstance(index, slice):
            real_indices = self._slice_real_indices(
                index=index, head=self._head, size=self._size, max_length=self._max_length,
            )
            return self._data[real_indices]

        index = self._normalize_index(index=index, size=self._size)
        if index < 0 or index >= self._size:
            raise IndexError("NumpyCircularBuffer index out of range")
        
        return self._data[self._real_index(logical_index=index)]  # type: ignore

    @overload
    def __setitem__(self, index: int, value: T, /) -> None: ...

    @overload
    def __setitem__(self, index: slice, value: Iterable[T], /) -> None: ...

    def __setitem__(self, index: Any, value: Any, /) -> None:
        if isinstance(index, slice):
            real_indices = self._slice_real_indices(
                index=index, head=self._head, size=self._size, max_length=self._max_length,
            )
            self._data[real_indices] = value
            return

        index = self._normalize_index(index=index, size=self._size)
        if index < 0 or index >= self._size:
            raise IndexError("NumpyCircularBuffer assignment index out of range")
        self._data[self._real_index(logical_index=index)] = value

    def __delitem__(self, index: int | slice, /) -> None:
        if isinstance(index, int):
            index = self._normalize_index(index=index, size=self._size)
            if index < 0 or index >= self._size:
                raise IndexError("NumpyCircularBuffer deletion index out of range")
            delete_indices = range(index, index + 1)
        elif isinstance(index, slice):
            start, stop, step = index.indices(self._size)
            delete_indices = range(start, stop, step)
        else:
            raise TypeError(
                f"NumpyCircularBuffer indices must be integers or slices, "
                f"not {type(index).__name__}"
            )

        if not delete_indices:
            return

        indices_to_remove = set(delete_indices)
        
        # Shift elements to fill gaps from deletion
        keep_mask = np.ones(shape=self._size, dtype=bool)
        for i in indices_to_remove:
            keep_mask[i] = False
            
        real_indices = np.array([self._real_index(logical_index=i) for i in range(self._size)])
        ordered_data = self._data[real_indices]
        new_data = ordered_data[keep_mask]
        
        new_size = len(new_data)
        self._data[:new_size] = new_data
        
        self._head = 0
        self._size = new_size

    def __iter__(self) -> Iterator[T]:
        for i in range(self._size):
            yield self._data[self._real_index(logical_index=i)]  # type: ignore

    def __contains__(self, value: object) -> bool:
        return any(item == value for item in self)

    def __str__(self) -> str:
        elements = ", ".join([str(item) for item in self])
        return f"NumpyCircularBuffer[{elements}]"
    
    def __repr__(self) -> str:
        return (
            f"NumpyCircularBuffer(max_length={self._max_length}, "
            f"dtype={self._data_type}, "
            f"items=[{', '.join(map(str, self))}])"
        )

    @override
    def append(self, item: T) -> None:
        item = item if item is not None else np.nan  # type: ignore[assignment]
        if self._size < self._max_length:
            self._data[self._real_index(logical_index=self._size)] = item
            self._size += 1
        else:
            self._data[self._head] = item
            self._head = (self._head + 1) % self._max_length

    @override
    def extend(self, iterable: Iterable[T]) -> None:
        for item in iterable:
            self.append(item)

    @override
    def insert(self, index: int, value: T) -> None:
        if self._size == self._max_length:
             raise IndexError("NumpyCircularBuffer is full, cannot insert (use append to overwrite)")
        
        index = self._normalize_index(index=index, size=self._size)
        if index < 0:  
            index = 0
        elif index > self._size: 
            index = self._size

        for i in range(self._size, index, -1):
             self._data[self._real_index(logical_index=i)] = self._data[
                self._real_index(logical_index=i - 1)
            ]
        self._data[self._real_index(logical_index=index)] = value
        self._size += 1

    @override
    def pop(self, index: int = -1) -> T:
        if self._size == 0:
            raise IndexError("pop from empty buffer")
        value = self[index]
        del self[index]
        return value

    def popleft(self) -> T:
        if self._size == 0:
            raise IndexError("pop from empty buffer")
        item = self._data[self._head]
        self._head = (self._head + 1) % self._max_length
        self._size -= 1
        return item  # type: ignore

    @override
    def remove(self, value: T) -> None:
        index = self.index(value=value)
        del self[index]

    @override
    def clear(self) -> None:
        self._head = 0
        self._size = 0

    @override
    def index(
        self, value: T, start: int = 0, stop: Optional[int] = None,
    ) -> int:
        if stop is None: 
            stop = self._size
        
        start = self._normalize_index(index=start, size=self._size)
        stop = self._normalize_index(index=stop, size=self._size)
        if start < 0: 
            start = 0
        if stop > self._size: 
            stop = self._size
        
        for i in range(start, stop):
             if self[i] == value:
                 return i
        raise ValueError(f"{value} is not in buffer")

    @override
    def count(self, value: T) -> int:
        return sum(1 for item in self if item == value)

    def copy(self) -> NumpyCircularBuffer[T]:
        new_buffer = NumpyCircularBuffer[T](
            max_length=self._max_length, data_type=self._data_type,
        )
        new_buffer.extend(self)
        return new_buffer
    
    def to_numpy(self) -> np.ndarray:
        real_indices = [self._real_index(logical_index=i) for i in range(self._size)]
        return self._data[real_indices]

    def _real_index(self, logical_index: int) -> int:
        return (self._head + logical_index) % self._max_length

    @staticmethod
    def _normalize_index(index: int, size: int) -> int:
        if index < 0:
            return index + size
        return index
    
    @staticmethod
    def _slice_real_indices(index: slice, head: int, size: int, max_length: int) -> np.ndarray:
        start, stop, step = index.indices(size)
        logical_indices = np.arange(start, stop, step)
        real_indices = (head + logical_indices) % max_length
        return real_indices