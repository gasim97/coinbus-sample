from enum import Enum, unique


@unique
class MsgGroup(Enum):

    ACCOUNT = 0
    ALGO = 1
    CONTROL = 2
    CORE = 3
    MARKETDATA = 4
    NOTIFICATION = 5
    ORDERENTRY = 6
    PINGPONG = 7
    REFERENCEDATA = 8
    SIGNAL = 9
    TIMER = 10

    def __str__(self) -> str:
        return self.name