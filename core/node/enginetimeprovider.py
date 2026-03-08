from common.utils.time import nanos_to_micros, nanos_to_millis, nanos_to_seconds
from core.msg.base.msg import Msg


class EngineTimeProvider:

    __slots__ = ('_engine_time')

    def __init__(self):
        self._engine_time = 0
    
    def on_message(self, msg: Msg) -> None:
        self._engine_time = msg.context.engine_time
    
    @property
    def engine_time(self) -> int:
        if self._engine_time == 0:
            raise ValueError("Attempted to get engine time, but it is not set")
        return self._engine_time
    
    @property
    def engine_time_seconds(self) -> int | float:
        return nanos_to_seconds(self.engine_time)
    
    @property
    def engine_time_millis(self) -> int | float:
        return nanos_to_millis(self.engine_time)
    
    @property
    def engine_time_micros(self) -> int | float:
        return nanos_to_micros(self.engine_time)
    
    @property
    def engine_time_nanos(self) -> int:
        return self.engine_time