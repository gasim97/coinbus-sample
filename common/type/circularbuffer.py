from __future__ import annotations

from typing import Any, Iterable, Iterator, MutableSequence, Optional, Sequence, TypeVar, overload


T = TypeVar('T')


class CircularBufferView(Sequence[T]):

    __slots__ = ('_buffer', '_start', '_stop', '_step', '_len')

    def __init__(self, buffer: CircularBuffer[T], start: int, stop: int, step: int) -> None:
        self._buffer = buffer
        self._start = start
        self._stop = stop
        self._step = step
        self._len = len(range(start, stop, step))

    def __len__(self) -> int:
        return self._len

    @overload
    def __getitem__(self, index: int) -> T: ...

    @overload
    def __getitem__(self, index: slice) -> Sequence[T]: ...

    def __getitem__(self, index: int | slice) -> T | Sequence[T]:
        if isinstance(index, slice):
            current_range = range(self._start, self._stop, self._step)
            new_range = current_range[index]
            return CircularBufferView(
                buffer=self._buffer, 
                start=new_range.start, 
                stop=new_range.stop, 
                step=new_range.step
            )
        
        index = CircularBuffer.normalize_index(index=index, size=self._len)
        if index < 0 or index >= self._len:
            raise IndexError("CircularBufferView index out of range")
            
        buffer_logical_index = self._start + (index * self._step)
        return self._buffer[buffer_logical_index]

    def __iter__(self) -> Iterator[T]:
        for i in range(self._len):
            yield self[i]

    def __str__(self) -> str:
        elements = ", ".join([str(item) for item in self])
        return f"CircularBufferView[{elements}]"


class CircularBuffer(MutableSequence[T]):

    __slots__ = ('_max_length', '_data', '_head', '_size')

    def __init__(self, max_length: int) -> None:
        if max_length <= 0:
            raise ValueError("max_length must be positive")
        self._max_length = max_length
        self._data: list[Optional[T]] = [None] * max_length
        self._head = 0
        self._size = 0

    def __len__(self) -> int:
        return self._size

    @overload
    def __getitem__(self, index: int, /) -> T: ...

    @overload
    def __getitem__(self, index: slice, /) -> MutableSequence[T]: ...

    def __getitem__(self, index: Any, /) -> T | MutableSequence[T]:
        if isinstance(index, slice):
            start, stop, step = index.indices(self._size)
            # FIXME: the view is not mutable
            return CircularBufferView(buffer=self, start=start, stop=stop, step=step)  # type: ignore

        index = self.normalize_index(index=index, size=self._size)
        if index < 0 or index >= self._size:
            raise IndexError("CircularBuffer index out of range")
        
        return self._data[self._real_index(logical_index=index)]  # type: ignore
    
    @overload
    def __setitem__(self, index: int, value: T, /) -> None: ... 
    
    @overload
    def __setitem__(self, index: slice, value: Iterable[T], /) -> None: ... 

    def __setitem__(self, index: Any, value: Any, /) -> None:
        if isinstance(index, slice):
            start, stop, step = index.indices(self._size)
            for i, item in enumerate(value):
                self[start + i * step] = item
            return
        
        index = self.normalize_index(index=index, size=self._size)
        if index < 0 or index >= self._size:
            raise IndexError("CircularBuffer assignment index out of range")
        
        self._data[self._real_index(logical_index=index)] = value

    def __delitem__(self, index: int | slice, /) -> None:
        if isinstance(index, int):
            index = self.normalize_index(index=index, size=self._size)
            if index < 0 or index >= self._size:
                raise IndexError("CircularBuffer deletion index out of range")
            delete_indices = range(index, index + 1)
        elif isinstance(index, slice):
            start, stop, step = index.indices(self._size)
            delete_indices = range(start, stop, step)
        else:
            raise TypeError(f"CircularBuffer indices must be integers or slices, not {type(index).__name__}")

        if not delete_indices:
            return

        indices_to_remove = set(delete_indices)
        write_index = 0
        original_size = self._size
        
        for read_index in range(original_size):
            if read_index not in indices_to_remove:
                if read_index != write_index:
                    self._data[self._real_index(logical_index=write_index)] = self._data[
                        self._real_index(logical_index=read_index)
                    ]
                write_index += 1
        
        for i in range(write_index, original_size):
            self._data[self._real_index(logical_index=i)] = None
        self._size = write_index

    def __iter__(self) -> Iterator[T]:
        for i in range(self._size):
            yield self._data[self._real_index(logical_index=i)]  # type: ignore

    def __contains__(self, value: object) -> bool:
        return any(item == value for item in self)

    def __str__(self) -> str:
        elements = ", ".join([str(item) for item in self])
        return f"CircularBuffer[{elements}]"
    
    def __repr__(self) -> str:
        return f"CircularBuffer(max_length={self._max_length}, items=[{', '.join(map(str, self))}])"

    def append(self, item: T) -> None:
        """
        Append an item to the end of the buffer.
        If the buffer is full, the oldest item is overwritten.
        """
        if self._size < self._max_length:
            self._data[self.normalize_index(index=self._size, size=self._max_length)] = item
            self._size += 1
        else:
            self._data[self.normalize_index(index=self._head, size=self._max_length)] = item
            self._head = (self._head + 1) % self._max_length

    def extend(self, iterable: Iterable[T]) -> None:
        for item in iterable:
            self.append(item)

    def insert(self, index: int, value: T) -> None:
        if self._size == self._max_length:
             raise IndexError("CircularBuffer is full, cannot insert (use append to overwrite)")
        
        index = self.normalize_index(index=index, size=self._size)
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

    def pop(self, index: int = -1) -> T:
        if self._size == 0:
            raise IndexError("pop from empty buffer")
        
        val = self[index]
        del self[index]
        return val

    def popleft(self) -> T:
        if self._size == 0:
            raise IndexError("pop from empty buffer")
        
        item = self._data[self._head]
        self._data[self._head] = None
        self._head = (self._head + 1) % self._max_length
        self._size -= 1
        return item  # type: ignore

    def remove(self, value: T) -> None:
        index = self.index(value)      
        for i in range(index, self._size - 1):
             self._data[self._real_index(logical_index=i)] = self._data[
                self._real_index(logical_index=i + 1)
            ]
        
        self._data[self._real_index(logical_index=self._size - 1)] = None
        self._size -= 1

    def clear(self) -> None:
        self._data = [None] * self._max_length
        self._head = 0
        self._size = 0

    def index(
        self, value: T, start: int = 0, stop: Optional[int] = None,
    ) -> int:
        if stop is None: 
            stop = self._size

        start = self.normalize_index(index=start, size=self._size)
        stop = self.normalize_index(index=stop, size=self._size)
        if start < 0: 
            start = 0
        if stop > self._size: 
            stop = self._size
        
        for i in range(start, stop):
             if self[i] == value:
                 return i
        raise ValueError(f"{value} is not in buffer")

    def count(self, value: T) -> int:
        return sum(1 for item in self if item == value)

    def copy(self) -> CircularBuffer[T]:
        new_buffer = CircularBuffer[T](self._max_length)
        new_buffer.extend(self)
        return new_buffer

    @staticmethod
    def normalize_index(index: int, size: int) -> int:
        if index < 0:
            return index + size
        return index

    def _real_index(self, logical_index: int) -> int:
        return (self._head + logical_index) % self._max_length
