import os

from abc import abstractmethod
from typing import Generic, TypeVar, override

from application.filelogger.utils.writer.writer import Writer
from common.utils.datetime import seconds_timestamp_to_datetime
from common.utils.time import nanos_to_seconds
from core.msg.base.msg import Msg
from core.node.node import ReadOnlyNode
from generated.type.core.enum.environment import EnvironmentEnum


W = TypeVar('W', bound=Writer)


class FileLoggerNode(ReadOnlyNode, Generic[W]):

    _FILE_DIRECTORY = "out"

    __slots__ = ('_pending_messages')

    def __init__(self, name: str = "FILELOGGER", environment: EnvironmentEnum = EnvironmentEnum.TEST):
        super().__init__(name=name, environment=environment)
        self._pending_messages: list[Msg] = []
        super().subscription_manager.subscribe_all(callback=self.handle_msg)
    
    @override
    def on_activated(self) -> None:
        self._create_file_directory()
        activated_datetime = seconds_timestamp_to_datetime(timestamp=nanos_to_seconds(super().engine_time_provider.engine_time))
        file_name = f"{self.environment.name}-{activated_datetime}-{self.name}"
        self.writer.open(directory=self._FILE_DIRECTORY, file_name=file_name)

    @override
    def on_deactivated(self) -> None:
        ...

    @override
    def on_session_ended(self) -> None:
        self.writer.close()
    
    @property
    @abstractmethod
    def writer(self) -> W:
        ...
    
    def handle_msg(self, msg: Msg) -> None:
        if not self.writer.is_open:
            self._pending_messages.append(msg.clone())
            return
        self._write_pending_msgs()
        self.writer.write_msg(msg=msg)
    
    def _create_file_directory(self) -> None:
        os.makedirs(name=self._FILE_DIRECTORY, exist_ok=True)
    
    def _write_pending_msgs(self) -> None:
        if len(self._pending_messages) == 0:
            return
        for msg in self._pending_messages:
            self.writer.write_msg(msg=msg)
        self._pending_messages.clear()