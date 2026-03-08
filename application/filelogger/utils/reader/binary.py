from __future__ import annotations

import struct

from io import BufferedReader
from typing import Any, Generator, Optional

from common.utils.pool import ObjectPoolManager
from core.msg.base.msg import Msg
from core.msg.msgserializer import MsgSerializer


class BinaryReader:
    """
    Utility class to read and deserialize binary log files created by BinaryFileLoggerNode
    """

    _MAGIC = b"BINLOG"
    _VERSION = 1
    _MAGIC_BYTES = 6
    _VERSION_BYTES = 1
    _MSG_LENGTH_BYTES = 2

    __slots__ = ('_file_path', '_pool_manager', '_file')
    
    def __init__(self, file_path: str, pool_manager: Optional[ObjectPoolManager] = None):
        self._file_path = file_path
        self._pool_manager = pool_manager or ObjectPoolManager()
        self._file: Optional[BufferedReader] = None
    
    def __enter__(self) -> BinaryReader:
        self._file = open(self._file_path, "rb")
        # Read and validate header
        magic = self._file.read(self._MAGIC_BYTES)
        if magic != self._MAGIC:
            raise ValueError("Invalid binary log file format")
        version = struct.unpack("B", self._file.read(self._VERSION_BYTES))[0]
        if version != self._VERSION:
            raise ValueError(f"Unsupported binary log version: {version}")
        return self
    
    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        if self._file is not None:
            self._file.close()
    
    def _read_message(self) -> Optional[bytes]:
        # Read message length
        assert self._file is not None
        length_bytes = self._file.read(self._MSG_LENGTH_BYTES)
        if not length_bytes:
            return None  # End of file
        
        msg_length = struct.unpack("H", length_bytes)[0]
        
        # Read the serialized message
        serialized_msg = self._file.read(msg_length)
        if len(serialized_msg) != msg_length:
            return None  # Incomplete message
        
        return serialized_msg
    
    def read_messages(self) -> Generator[Msg, None, None]:
        while True:
            serialized_msg = self._read_message()
            if serialized_msg is None:
                break
            msg = MsgSerializer.deserialize(serialized=serialized_msg, pool_manager=self._pool_manager)
            if msg is not None:
                with msg:
                    yield msg 
    
    def read_messages_as_dicts(self) -> Generator[dict[str, Any], None, None]:
        while True:
            serialized_msg = self._read_message()
            if serialized_msg is None:
                break
            deserialized = MsgSerializer.unpack(serialized=serialized_msg)
            if deserialized:
                yield deserialized