from __future__ import annotations

from typing import Generic, Iterator, Optional, Self, TypeVar


T = TypeVar('T')


class LinkedListNode(Generic[T]):

    __slots__ = ('_data', '_previous', '_next')

    def __init__(
        self, 
        data: Optional[T] = None, 
        previous: Optional[LinkedListNode[T]] = None,
        next: Optional[LinkedListNode[T]] = None,
    ):
        self._data: Optional[T] = data
        self._previous: Optional[LinkedListNode[T]] = previous
        self._next: Optional[LinkedListNode[T]] = next
    
    @property
    def data(self) -> Optional[T]:
        return self._data
    
    @data.setter
    def data(self, data: Optional[T]) -> None:
        self._data = data
    
    def link(self, previous: Optional[LinkedListNode[T]] = None, next: Optional[LinkedListNode[T]] = None) -> Self:
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

    def clear(self) -> Self:
        self._data = None
        self._previous = None
        self._next = None
        return self
    
    def __str__(self) -> str:
        return f"LinkedList.Node(data={self._data})"


class UnpooledLinkedList(Generic[T]):

    __slots__ = ('_head', '_tail', '_size')

    def __init__(self):
        self._head: LinkedListNode[T] = LinkedListNode[T]()
        self._tail: LinkedListNode[T] = LinkedListNode[T]().link(previous=self._head)
        self._size = 0
    
    @property
    def size(self) -> int:
        return self._size
    
    @property
    def is_empty(self) -> bool:
        return self._size == 0
    
    @property
    def is_not_empty(self) -> bool:
        return self._size > 0
    
    @property
    def head(self) -> Optional[LinkedListNode[T]]:
        if self._head._next is self._tail or self._head._next is None:
            return None
        return self.remove(self._head._next)
    
    @head.setter
    def head(self, node: LinkedListNode[T]) -> None:
        node.link(previous=self._head, next=self._head._next)
        self._size += 1
    
    @property
    def tail(self) -> Optional[LinkedListNode[T]]:
        if self._tail._previous is self._head or self._tail._previous is None:
            return None
        return self.remove(self._tail._previous)
    
    @tail.setter
    def tail(self, node: LinkedListNode[T]) -> None:
        node.link(previous=self._tail._previous, next=self._tail)
        self._size += 1
    
    @property
    def peek_head(self) -> Optional[LinkedListNode[T]]:
        return self._head._next
    
    @property
    def peek_tail(self) -> Optional[LinkedListNode[T]]:
        return self._tail._previous

    def remove(self, node: LinkedListNode[T]) -> LinkedListNode[T]:
        self._size -= 1
        return node.unlink()
    
    def clear(self) -> Self:
        self._head._next = None
        self._tail._previous = None
        self._size = 0
        return self
    
    def __len__(self) -> int:
        return self._size
    
    def __iter__(self) -> Iterator[LinkedListNode[T]]:
        """Iterate over the elements of the linked list"""
        node = self.peek_head
        while node is not None:
            yield node
            node = node._next

    def __contains__(self, item: LinkedListNode[T]) -> bool:
        node = self.peek_head
        while node is not None:
            if node == item:
                return True
            node = node._next
        return False
    
    def __str__(self) -> str:
        elements = ", ".join([str(node.data) for node in self])
        return f"LinkedList[{elements}]"


class LinkedList(Generic[T]):

    __slots__ = ('_linked_list', '_node_pool')

    def __init__(
        self, 
        expected_size: Optional[int] = None, 
        unpooled_linked_list: Optional[UnpooledLinkedList[T]] = None,
        node_pool: Optional[UnpooledLinkedList[T]] = None,
    ):
        super().__init__()
        self._linked_list = unpooled_linked_list or UnpooledLinkedList[T]()
        self._node_pool = node_pool or UnpooledLinkedList[T]()
        if expected_size is not None:
            while self._node_pool.size < expected_size:
                self._node_pool.head = LinkedListNode[T]()
    
    @property
    def size(self) -> int:
        return self._linked_list.size
    
    @property
    def is_empty(self) -> bool:
        return self._linked_list.is_empty
    
    @property
    def is_not_empty(self) -> bool:
        return self._linked_list.is_not_empty
    
    @property
    def head(self) -> Optional[T]:
        node = self._linked_list.head
        if node is None:
            return None
        item = node.data
        self._node_pool.head = node.clear()
        return item
    
    @head.setter
    def head(self, data: Optional[T]) -> None:
        node = self._node_pool.head or LinkedListNode[T]()
        node.data = data
        self._linked_list.head = node
    
    @property
    def tail(self) -> Optional[T]:
        node = self._linked_list.tail
        if node is None:
            return None
        item = node.data
        self._node_pool.head = node.clear()
        return item
    
    @tail.setter
    def tail(self, data: Optional[T]) -> None:
        node = self._node_pool.head or LinkedListNode[T]()
        node.data = data
        self._linked_list.tail = node
    
    @property
    def peek_head(self) -> Optional[T]:
        node = self._linked_list.peek_head
        return node.data if node is not None else None
    
    @property
    def peek_tail(self) -> Optional[T]:
        node = self._linked_list.peek_tail
        return node.data if node is not None else None
    
    def clear(self) -> Self:
        next = self._linked_list.peek_head
        while next is not None:
            current = next
            next = current._next
            self._node_pool.head = current.clear()
        self._linked_list.clear()
        return self
    
    def __len__(self) -> int:
        return len(self._linked_list)
    
    def __iter__(self) -> Iterator[Optional[T]]:
        node = self._linked_list.peek_head
        while node is not None:
            yield node.data
            node = node._next

    def __contains__(self, item: Optional[T]) -> bool:
        node = self._linked_list.peek_head
        while node is not None:
            if node.data == item:
                return True
            node = node._next
        return False
    
    def __str__(self) -> str:
        elements = ", ".join([str(item) for item in self])
        return f"LinkedList[{elements}]"