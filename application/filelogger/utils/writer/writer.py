from abc import ABC, abstractmethod

from typing import Self

from core.msg.base.msg import Msg


class Writer(ABC):
    
    @property
    @abstractmethod
    def is_open(self) -> bool:
        ...
    
    @abstractmethod
    def open(self, directory: str, file_name: str) -> Self:
        ...
    
    @abstractmethod
    def write_msg(self, msg: Msg) -> bool:
        ...
    
    @abstractmethod
    def flush(self) -> None:
        ...
    
    @abstractmethod
    def close(self) -> None:
        ...