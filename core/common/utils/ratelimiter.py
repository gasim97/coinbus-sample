from collections import deque
from typing import Callable, Generic, Optional, TypeVar, override

from core.node.callbackscheduler import CallbackScheduler, ScheduledCallback


T = TypeVar('T')


class RateLimiter(Generic[T], ScheduledCallback):

    __slots__ = ('_callback_scheduler', '_frequency_nanos', '_callback, _pending_callback_args', '_callback_scheduler_id')

    def __init__(self, callback_scheduler: CallbackScheduler, frequency_nanos: int, callback: Callable[[T], None]):
        self._callback_scheduler = callback_scheduler
        self._frequency_nanos = frequency_nanos
        self._callback = callback
        self._pending_callback_args = deque[T]()
        self._callback_scheduler_id: Optional[int] = None
    
    def __call__(self, arg: T) -> None:
        self._pending_callback_args.append(arg)
    
    def start(self) -> None:
        self._callback_scheduler_id = self._callback_scheduler.schedule_at_fixed_rate(
            callback=self, frequency_nanos=self._frequency_nanos,
        )
    
    def stop(self) -> None:
        if self._callback_scheduler_id is not None:
            self._callback_scheduler.cancel(callback_id=self._callback_scheduler_id)
    
    @override
    def _on_scheduled_time(self, callback_id: int) -> None:
        while len(self._pending_callback_args) > 0:
            self._callback(self._pending_callback_args.popleft())