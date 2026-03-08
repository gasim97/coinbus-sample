from numba import njit
from typing import Optional, Sequence, TypeVar

import numpy as np


T = TypeVar("T", bound=int | float)


def max_value(values: Sequence[T]) -> T:
    return np.max(values)


def min_value(values: Sequence[T]) -> T:
    return np.min(values)


@njit
def mid_value(values: Sequence[T]) -> float:
    min = float("inf")
    max = float("-inf")
    for value in values:
        if value < min:
            min = value
        if value > max:
            max = value
    return (min + max) / 2


def weighted_mean(
    values: Sequence[T], 
    weights: Sequence[T],
) -> float:
    assert len(values) == len(weights), "Values and weights must have the same length"
    return np.average(values, weights=weights)  # type: ignore[return-value]


def weighted_sum(
    values: Sequence[T], 
    weights: Sequence[T],
) -> float:
    assert len(values) == len(weights), "Values and weights must have the same length"
    return np.dot(values, weights)


def mean(values: Sequence[T]) -> float:
    return np.mean(values)  # type: ignore[return-value]


def median(values: Sequence[T]) -> float:
    return np.median(values)  # type: ignore[return-value]


@njit
def exponential_moving_average(
    values: Sequence[T], 
    decay_factor: float,
    last_value: Optional[T] = None,
) -> Optional[float]:
    if len(values) == 0:
        return None
    alpha = decay_factor
    one_minus_alpha = 1 - alpha
    ema: float = last_value if last_value is not None else values[0]
    for i in range(len(values)):
        ema = alpha * values[i] + one_minus_alpha * ema
    return ema


@njit
def exponential_moving_average_vector(
    values: Sequence[T], 
    decay_factor: float,
    last_value: Optional[T] = None,
) -> list[float]:
    alpha = decay_factor
    one_minus_alpha = 1 - alpha
    ema: list[float] = [last_value if last_value is not None else values[0]]
    for i in range(len(values)):
        ema.append(alpha * values[i] + one_minus_alpha * ema[-1])
    return ema[1:]


def value_pct_diff(value_1: T, value_2: T) -> float:
    return ((value_1 / value_2) - 1) * 100  # type: ignore[operator]