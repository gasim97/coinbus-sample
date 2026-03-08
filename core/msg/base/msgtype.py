from enum import Enum, unique


@unique
class MsgType(Enum):
    ...

    def __str__(self) -> str:
        return self.name