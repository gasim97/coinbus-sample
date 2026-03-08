import os

from io import TextIOWrapper
from typing import Optional, Self, override

from application.filelogger.utils.writer.writer import Writer
from common.utils.datetime import nanos_timestamp_to_time
from core.msg.base.msg import Msg


class TextWriter(Writer):
    """
    Utility class to write text data to a file.
    """

    __slots__ = ('_file')

    def __init__(self):
        self._file: Optional[TextIOWrapper] = None
    
    @override
    @property
    def is_open(self) -> bool:
        return self._file is not None
    
    @override
    def open(self, directory: str, file_name: str) -> Self:
        self._file = open(os.path.join(directory, f"{file_name}.txt"), "w")
        return self
    
    @override
    def write_msg(self, msg: Msg) -> bool:
        return self.write_text(f"{nanos_timestamp_to_time(timestamp=msg.context.engine_time)} {msg}\n")
    
    @override
    def flush(self) -> None:
        if self.is_open:
            self._file.flush()  # type: ignore[union-attr]
    
    @override
    def close(self) -> None:
        self.flush()
        if self.is_open:
            self._file.close()  # type: ignore[union-attr]
            self._file = None
    
    def write_text(self, text: str) -> bool:
        if self.is_open:
            self._file.write(text)  # type: ignore[union-attr]
            return True
        return False