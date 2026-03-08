from abc import ABC, abstractmethod

from typing import Callable, Optional, override

from core.msg.base.msg import Msg
from core.node.enginetimeprovider import EngineTimeProvider


class ScheduledCallback(ABC):

    @abstractmethod
    def _on_scheduled_time(self, callback_id: int) -> None:
        """Callback function invoked at a scheduled time"""
        ...


class NoArgsFunctionScheduledCallback(ScheduledCallback):

    __slots__ = ('_callback')

    def __init__(self, callback: Callable[[], None]):
        self._callback = callback

    @override
    def _on_scheduled_time(self, callback_id: int) -> None:
        self._callback()


class CallbackHandler:

    __slots__ = ('_callback_id', '_callback', '_reference_time_nanos', '_frequency_nanos', '_iterations', '_completed_iterations')

    def __init__(
        self, 
        callback_id: int,
        callback: ScheduledCallback, 
        current_time_nanos: int, 
        frequency_nanos: float, 
        iterations: Optional[int] = None
    ):
        self._callback_id = callback_id
        self._callback = callback
        self._reference_time_nanos = current_time_nanos
        self._frequency_nanos = frequency_nanos
        self._iterations = iterations
        self._completed_iterations = 0
    
    @property
    def id(self) -> int:
        return self._callback_id
    
    @property
    def is_complete(self) -> bool:
        return self._iterations is not None and self._completed_iterations >= self._iterations
    
    def on_time(self, time_nanos: int) -> None:
        if self.is_complete:
            return
        time_diff = time_nanos - self._reference_time_nanos
        if time_diff < self._frequency_nanos:
            return
        self._reference_time_nanos = time_nanos
        self._completed_iterations += 1
        self._callback._on_scheduled_time(callback_id=self._callback_id)


class CallbackScheduler:

    __slots__ = ('_engine_time_provider', '_callbacks_by_id', '_callback_id_generator')

    def __init__(self, engine_time_provider: EngineTimeProvider):
        self._engine_time_provider = engine_time_provider
        self._callbacks_by_id: dict[int, CallbackHandler] = {}
        self._callback_id_generator = 0

    def on_message(self, msg: Msg) -> None:
        callbacks = list(self._callbacks_by_id.values())
        for callback in callbacks:
            callback.on_time(time_nanos=msg.context.engine_time)
            if callback.is_complete:
                self.cancel(callback_id=callback.id)
    
    def schedule(self, callback: ScheduledCallback, delay_nanos: float) -> int:
        """Schedule a callback with a certain delay"""
        self._callback_id_generator += 1
        self._callbacks_by_id[self._callback_id_generator] = CallbackHandler(
            callback_id=self._callback_id_generator,
            callback=callback, 
            current_time_nanos=self._engine_time_provider.engine_time, 
            frequency_nanos=delay_nanos, 
            iterations=1,
        )
        return self._callback_id_generator
    
    def schedule_at_fixed_rate(self, callback: ScheduledCallback, frequency_nanos: float) -> int:
        """Schedule a callback at a certain frequency"""
        self._callback_id_generator += 1
        self._callbacks_by_id[self._callback_id_generator] = CallbackHandler(
            callback_id=self._callback_id_generator,
            callback=callback, 
            current_time_nanos=self._engine_time_provider.engine_time, 
            frequency_nanos=frequency_nanos,
        )
        return self._callback_id_generator
    
    def cancel(self, callback_id: int) -> None:
        if callback_id in self._callbacks_by_id:
            self._callbacks_by_id.pop(callback_id)
    
    def cancel_all(self) -> None:
        self._callbacks_by_id.clear()