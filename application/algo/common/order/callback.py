from typing import Callable, Generic, TypeVar, override

from application.orderentry.order.internalorder import InternalOrder
from core.node.callbackscheduler import ScheduledCallback


O = TypeVar('O', bound=InternalOrder)


class OrderCallback(Generic[O], ScheduledCallback):

    __slots__ = ('_callback', '_order')

    def __init__(self, callback: Callable[[str], None], order: O):
        self._callback = callback
        self._order = order

    @override
    def _on_scheduled_time(self, callback_id: int) -> None:
        if not self._order.is_complete:
            self._callback(self._order.internal_order_id)