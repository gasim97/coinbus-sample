from typing import override

from application.filelogger.node.filelogger import FileLoggerNode
from application.filelogger.utils.writer.text import TextWriter
from generated.type.core.enum.environment import EnvironmentEnum


class TextFileLoggerNode(FileLoggerNode[TextWriter]):
    """
    A node that logs messages to a text file.
    """

    __slots__ = ("_writer")

    def __init__(self, name: str = "TEXTFILELOGGER", environment: EnvironmentEnum = EnvironmentEnum.TEST):
        super().__init__(name=name, environment=environment)
        self._writer = TextWriter()
    
    @override
    @property
    def writer(self) -> TextWriter:
        return self._writer