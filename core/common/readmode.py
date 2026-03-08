from dataclasses import dataclass
from enum import Enum

from common.utils.time import millis_to_seconds


@dataclass
class ReadSettings:
    select_timeout_seconds: float


class ReadMode(ReadSettings, Enum):
    BUSY_SPIN = (0.0)
    BLOCKING = (millis_to_seconds(10))
    LONG_BLOCKING = (1.0)