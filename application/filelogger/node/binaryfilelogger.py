from typing import override

from application.filelogger.node.filelogger import FileLoggerNode
from application.filelogger.utils.writer.binary import BinaryWriter
from generated.type.core.enum.environment import EnvironmentEnum


class BinaryFileLoggerNode(FileLoggerNode[BinaryWriter]):
    """
    A node that logs messages to a binary file.
    """

    __slots__ = ("_writer")

    def __init__(self, name: str = "BINARYFILELOGGER", environment: EnvironmentEnum = EnvironmentEnum.TEST):
        super().__init__(name=name, environment=environment)
        self._writer = BinaryWriter()

    @override
    @property
    def writer(self) -> BinaryWriter:
        return self._writer