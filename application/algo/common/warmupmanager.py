from typing import Callable, Optional, override

from core.node.callbackscheduler import CallbackScheduler, ScheduledCallback


class WarmupManager(ScheduledCallback):

    __slots__ = ('_callback_scheduler', '_on_warmup_completed', '_warmup_completed', '_warmup_callback_id')

    def __init__(self, callback_scheduler: CallbackScheduler, on_warmup_completed: Optional[Callable[[], None]] = None):
        self._callback_scheduler = callback_scheduler
        self._on_warmup_completed = on_warmup_completed
        self._warmup_completed = False
        self._warmup_callback_id: Optional[int] = None

    @property
    def warmup_completed(self) -> bool:
        return self._warmup_completed

    @override
    def _on_scheduled_time(self, callback_id: int) -> None:
        self._warmup_completed = True
        if self._on_warmup_completed is not None:
            self._on_warmup_completed()

    def start(self, delay_nanos: int) -> None:
        if self._warmup_callback_id is not None:
            self._callback_scheduler.cancel(callback_id=self._warmup_callback_id)
        self._warmup_callback_id = self._callback_scheduler.schedule(callback=self, delay_nanos=delay_nanos)
        self._warmup_completed = False