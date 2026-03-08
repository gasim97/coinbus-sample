from __future__ import annotations

from abc import ABC, abstractmethod
from collections import deque
from logging import Logger
from multiprocessing.connection import Connection
from threading import Thread, Lock, Event
from typing import Callable, Optional, Type, TypeVar, override

from common.utils.pipe import is_connection_ready_for_write, safe_write_to_pipe_connection
from common.utils.pool import ObjectPoolManager
from core.msg.base.msg import Msg
from core.msg.msgserializer import MsgSerializer


M = TypeVar('M', bound=Msg)


class _ThreadedMessageSenderHandler(ABC):

    @abstractmethod
    def _next_message_to_send(self) -> Optional[Msg]:
        ...
    
    @abstractmethod
    def _on_send_failed(self, msg: Msg) -> None:
        ...


class _ThreadedMessageSender:
    """Utility class to handle asynchronous message sending in a separate thread"""

    _CONNECTION_READY_TIMEOUT_SECONDS = 1

    __slots__ = (
        '_logger',
        '_handler',
        '_activated',
        '_write_connection',
        '_send_thread',
        '_send_thread_lock',
        '_send_event',
    )

    def __init__(self, logger: Logger, handler: _ThreadedMessageSenderHandler):
        self._logger = logger
        self._handler = handler
        self._activated: bool = False
        self._write_connection: Optional[Connection] = None
        self._send_thread: Optional[Thread] = None
        self._send_thread_lock: Optional[Lock] = None
        self._send_event: Optional[Event] = None

    @property
    def is_send_queue_full(self) -> bool:
        return not is_connection_ready_for_write(
            connection=self._write_connection,  # type: ignore[arg-type]
            timeout_seconds=self._CONNECTION_READY_TIMEOUT_SECONDS,
        )

    def set_write_connection(self, connection: Connection) -> None:
        self._write_connection = connection
    
    def on_session_started(self) -> None:
        self._send_thread_lock = Lock()
        self._send_event = Event()

    def set_activated(self, activated: bool) -> None:
        self._activated = activated
        if self._activated:
            self._start()
        else:
            self._stop()

    def send(self) -> None:
        """Notify the send thread that there are messages to process"""
        if self._send_event is not None:
            self._send_event.set()
    
    def _wait(self) -> None:
        if self._send_event is not None:
            self._send_event.wait()
            self._send_event.clear()

    def _start(self) -> None:
        with self._send_thread_lock:  # type: ignore[union-attr]
            if self._send_thread is not None:
                return
            self._send_thread = Thread(target=self._run, daemon=True)
            self._send_thread.start()

    def _stop(self) -> None:
        with self._send_thread_lock:  # type: ignore[union-attr]
            if self._send_thread is None:
                return
            self.send()
            self._send_thread.join()
            self._send_thread = None

    def _run(self) -> None:
        while self._activated:
            msg = self._handler._next_message_to_send()
            if msg is None:
                self._wait()
                continue
            if not self._dispatch_msg(msg=msg):
                self._logger.error(f"Failed to send message: {msg}")
                self._handler._on_send_failed(msg)
            
    def _dispatch_msg(self, msg: Msg) -> bool:
        while self._activated:
            if self.is_send_queue_full:
                continue
            try:
                return safe_write_to_pipe_connection(
                    connection=self._write_connection,  # type: ignore[arg-type]
                    payload=MsgSerializer.serialize(msg=msg),
                    logger=self._logger,
                )
            except Exception:
                self._logger.error("Failed to send message", exc_info=True)
                return False
        return False


class MessageSender(_ThreadedMessageSenderHandler):

    __slots__ = (
        '_node_name',
        '_logger',
        '_pool_manager',
        '_activated',
        '_messages_in_flight_queue',
        '_out_queue',
        '_no_messages_in_flight_callback',
        '_threaded_message_sender',
    )

    def __init__(
        self, node_name: str, logger: Logger, pool_manager: ObjectPoolManager,
    ):
        self._node_name = node_name
        self._logger = logger
        self._pool_manager = pool_manager
        self._activated = False
        self._messages_in_flight_queue = deque[Msg]()
        self._out_queue = deque[Msg]()
        self._no_messages_in_flight_callback: Optional[Callable[[], None]] = None
        self._threaded_message_sender = _ThreadedMessageSender(logger=logger, handler=self)
    
    @property
    def messages_in_flight(self) -> int:
        """The number of messages sent, but not yet broadcast by the engine"""
        return len(self._messages_in_flight_queue)
    
    @property
    def has_messages_in_flight(self) -> bool:
        return self.messages_in_flight > 0

    def set_engine_write_connection(self, connection: Connection) -> None:
        self._threaded_message_sender.set_write_connection(connection)
    
    def on_session_started(self) -> None:
        self._threaded_message_sender.on_session_started()
    
    def set_activated(self, activated: bool) -> None:
        self._activated = activated
        self._threaded_message_sender.set_activated(activated)
        if self._activated:
            self._out_queue.extend(self._messages_in_flight_queue)
            self._threaded_message_sender.send()
        else:
            self._out_queue.clear()
    
    def set_no_messages_in_flight_callback(self, callback: Callable[[], None]) -> None:
        self._no_messages_in_flight_callback = callback
    
    def on_start_event_loop(self, msg: Msg) -> None:
        if msg.context.sender != self._node_name:
            return
        if not self.has_messages_in_flight:
            return
        inflight_msg = self._messages_in_flight_queue.popleft()
        assert msg.group == inflight_msg.group and msg.type == inflight_msg.type, (
            f"Acked message (seq: {msg.context.sequence}) group or type mismatch: "
            f"{msg.group} != {inflight_msg.group} or {msg.type} != {inflight_msg.type}"
        )
        inflight_msg.release()

    def on_end_event_loop(self, msg: Msg) -> None:
        if not self.has_messages_in_flight and self._no_messages_in_flight_callback is not None:
            self._no_messages_in_flight_callback()

    def send(self, msg: Msg) -> None:
        msg_clone = self.create(type=type(msg)).copy_from(other=msg)
        self._messages_in_flight_queue.append(msg_clone)
        if self._activated:
            self._out_queue.append(msg_clone)
        self._threaded_message_sender.send()
    
    def create(self, type: Type[M]) -> M:
        return self._pool_manager.pool(type=type).get()
    
    @override
    def _next_message_to_send(self) -> Optional[Msg]:
        try:
            return self._out_queue.popleft()
        except IndexError:
            return None
    
    @override
    def _on_send_failed(self, msg: Msg) -> None:
        try:
            self._messages_in_flight_queue.remove(msg)
        except ValueError:
            self._logger.warning(f"Message of failed send not found in flight queue: {msg}")
        msg.release()