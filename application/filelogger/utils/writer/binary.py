import os
import struct

from io import BufferedWriter
from typing import Optional, Self, override

from application.filelogger.utils.writer.writer import Writer
from core.msg.base.msg import Msg
from core.msg.msgserializer import MsgSerializer


class BinaryWriter(Writer):
    """
    Utility class to write binary data to a file.
    - **File Header**: Each binary log file includes:
        - Magic bytes: "BINLOG" (6 bytes)
        - Version: 1 (1 byte)
    - **Message Structure**: Each message is stored with:
        - Message length: 2-byte unsigned short (length of serialized message)
        - Message data: Variable-length MessagePack serialized message
    - **Example**:
        ```
        BINLOG\x01                     # Header
        \x00\x45                       # Message length (2 bytes)
        [69 bytes of MessagePack data] # Serialized message
        \x00\x32                       # Next message length (2 bytes)
        [50 bytes of MessagePack data] # Next serialized message
        ...
        ```
    """

    _MAGIC = b"BINLOG"
    _VERSION = 1

    __slots__ = ('_file')

    def __init__(self):
        self._file: Optional[BufferedWriter] = None
    
    @override
    @property
    def is_open(self) -> bool:
        return self._file is not None
    
    @override
    def open(self, directory: str, file_name: str) -> Self:
        self._file = open(os.path.join(directory, f"{file_name}.bin"), "wb")
        # Write file header
        self.write_bytes(self._MAGIC)  # Magic bytes
        self.write_bytes(struct.pack("B", self._VERSION))  # Version
        self.flush()
        return self

    @override
    def write_msg(self, msg: Msg) -> bool:
        serialized_msg = MsgSerializer.serialize(msg=msg)
        msg_length = len(serialized_msg)
        return (
            self.write_bytes(struct.pack("H", msg_length))  # 2-byte message length
            and self.write_bytes(serialized_msg)  # The serialized message
        )
    
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
    
    def write_bytes(self, data: bytes) -> bool:
        if self.is_open:
            self._file.write(data)  # type: ignore[union-attr]
            return True
        return False