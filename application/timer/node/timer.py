import time

from threading import Timer
from typing import Optional, override

from common.utils.time import millis_to_seconds, seconds_to_nanos
from core.node.node import ReadWriteNode
from generated.type.core.enum.environment import EnvironmentEnum
from generated.type.timer.msg.heartbeat import HeartbeatMsg


class TimerNode(ReadWriteNode):

    _FREQUENCY_S = millis_to_seconds(1)
    _FREQUENCY_NS = int(seconds_to_nanos(_FREQUENCY_S))
    _EVALUATE_FREQUANCY_THRESHOLD_INTERVAL_S = _FREQUENCY_S / 10

    __slots__ = ('_timer', '_last_engine_time_nanos', '_last_publish_time_nanos', '_last_receive_time_nanos', '_next_target_time_nanos')

    def __init__(self, name: str = "TIMER", environment: EnvironmentEnum = EnvironmentEnum.TEST):
        super().__init__(name=name, environment=environment)
        self._timer: Optional[Timer] = None
        self._last_engine_time_nanos: int = 0
        self._last_publish_time_nanos: int = 0
        self._last_receive_time_nanos: int = 0
        self._next_target_time_nanos: int = 0
        super().subscription_manager.subscribe_mine(type=HeartbeatMsg, callback=self._handle_heartbeat)
    
    @override
    def on_activated(self) -> None:
        self._next_target_time_nanos = time.time_ns() + self._FREQUENCY_NS
        self._set_timer()
    
    @override
    def on_deactivated(self) -> None:
        self._kill_timer()
    
    def _handle_heartbeat(self, msg: HeartbeatMsg) -> None:
        self._last_receive_time_nanos = time.time_ns()
        self._last_engine_time_nanos = msg.context.engine_time

    def _set_timer(self) -> None:
        self._timer = Timer(
            interval=self._EVALUATE_FREQUANCY_THRESHOLD_INTERVAL_S, 
            function=self._evaluate_frequency_threshold,
        )
        self._timer.start()
    
    def _kill_timer(self) -> None:
        if self._timer is not None:
            self._timer.cancel()
    
    def _evaluate_frequency_threshold(self) -> None:
        try:
            current_time_nanos = time.time_ns()
            if current_time_nanos >= self._next_target_time_nanos:
                self._send_heartbeat()
                while self._next_target_time_nanos <= current_time_nanos:
                    self._next_target_time_nanos += self._FREQUENCY_NS
        except:
            pass
        finally:
            self._set_timer()
    
    def _send_heartbeat(self) -> None:
        if super().message_sender.has_messages_in_flight:
            return
        with super().message_sender.create(type=HeartbeatMsg) as heartbeat_msg:
            heartbeat_msg.node_to_engine_msg_lag = self._last_engine_time_nanos - self._last_publish_time_nanos
            heartbeat_msg.engine_to_node_msg_lag = self._last_receive_time_nanos - self._last_engine_time_nanos
            heartbeat_msg.msg_ack_lag = self._last_receive_time_nanos - self._last_publish_time_nanos
            self._last_publish_time_nanos = time.time_ns()
            super().message_sender.send(msg=heartbeat_msg)