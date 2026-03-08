from typing import Optional, TypeVar


T = TypeVar('T')


def none_filter(values: list[Optional[T]]) -> list[T]:
    return [value for value in values if value is not None]