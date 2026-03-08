from typing import overload

from generated.type.common.constants import (
    DAYS_TO_HOURS,
    DAYS_TO_MICROS,
    DAYS_TO_MILLIS,
    DAYS_TO_MINUTES,
    DAYS_TO_NANOS,
    DAYS_TO_SECONDS,
    HOURS_TO_MICROS,
    HOURS_TO_MILLIS,
    HOURS_TO_MINUTES,
    HOURS_TO_NANOS,
    HOURS_TO_SECONDS,
    MICROS_TO_NANOS,
    MILLIS_TO_MICROS,
    MILLIS_TO_NANOS,
    MINUTES_TO_MICROS,
    MINUTES_TO_MILLIS,
    MINUTES_TO_NANOS,
    MINUTES_TO_SECONDS,
    SECONDS_TO_MICROS,
    SECONDS_TO_MILLIS,
    SECONDS_TO_NANOS,
)


def micros_to_nanos(micros: float) -> float:
    return micros * MICROS_TO_NANOS


def nanos_to_micros(nanos: float, trim: bool = False) -> int | float:
    return nanos // MICROS_TO_NANOS if trim else nanos / MICROS_TO_NANOS


@overload
def millis_to_nanos(millis: int) -> int: ...


@overload
def millis_to_nanos(millis: float) -> float: ...


def millis_to_nanos(millis: int | float) -> int | float:
    return millis * MILLIS_TO_NANOS


def nanos_to_millis(nanos: float, trim: bool = False) -> int | float:
    return nanos // MILLIS_TO_NANOS if trim else nanos / MILLIS_TO_NANOS


@overload
def millis_to_micros(millis: int) -> int: ...


@overload
def millis_to_micros(millis: float) -> float: ...


def millis_to_micros(millis: int | float) -> int | float:
    return millis * MILLIS_TO_MICROS


def micros_to_millis(micros: float, trim: bool = False) -> int | float:
    return micros // MILLIS_TO_MICROS if trim else micros / MILLIS_TO_MICROS


@overload
def seconds_to_nanos(seconds: int) -> int: ...


@overload
def seconds_to_nanos(seconds: float) -> float: ...


def seconds_to_nanos(seconds: int | float) -> int | float:
    return seconds * SECONDS_TO_NANOS


def nanos_to_seconds(nanos: float, trim: bool = False) -> int | float:
    return nanos // SECONDS_TO_NANOS if trim else nanos / SECONDS_TO_NANOS


@overload
def seconds_to_micros(seconds: int) -> int: ...


@overload
def seconds_to_micros(seconds: float) -> float: ...


def seconds_to_micros(seconds: int | float) -> int | float:
    return seconds * SECONDS_TO_MICROS


def micros_to_seconds(micros: float, trim: bool = False) -> int | float:
    return micros // SECONDS_TO_MICROS if trim else micros / SECONDS_TO_MICROS


@overload
def seconds_to_millis(seconds: int) -> int: ...


@overload
def seconds_to_millis(seconds: float) -> float: ...


def seconds_to_millis(seconds: int | float) -> int | float:
    return seconds * SECONDS_TO_MILLIS


def millis_to_seconds(millis: float, trim: bool = False) -> int | float:
    return millis // SECONDS_TO_MILLIS if trim else millis / SECONDS_TO_MILLIS


@overload
def minutes_to_nanos(minutes: int) -> int: ...


@overload
def minutes_to_nanos(minutes: float) -> float: ...


def minutes_to_nanos(minutes: int | float) -> int | float:
    return minutes * MINUTES_TO_NANOS


def nanos_to_minutes(nanos: float, trim: bool = False) -> int | float:
    return nanos // MINUTES_TO_NANOS if trim else nanos / MINUTES_TO_NANOS


@overload
def minutes_to_micros(minutes: int) -> int: ...


@overload
def minutes_to_micros(minutes: float) -> float: ...


def minutes_to_micros(minutes: int | float) -> int | float:
    return minutes * MINUTES_TO_MICROS


def micros_to_minutes(micros: float, trim: bool = False) -> int | float:
    return micros // MINUTES_TO_MICROS if trim else micros / MINUTES_TO_MICROS


@overload
def minutes_to_millis(minutes: int) -> int: ...


@overload
def minutes_to_millis(minutes: float) -> float: ...


def minutes_to_millis(minutes: int | float) -> int | float:
    return minutes * MINUTES_TO_MILLIS


def millis_to_minutes(millis: float, trim: bool = False) -> int | float:
    return millis // MINUTES_TO_MILLIS if trim else millis / MINUTES_TO_MILLIS


@overload
def minutes_to_seconds(minutes: int) -> int: ...


@overload
def minutes_to_seconds(minutes: float) -> float: ...


def minutes_to_seconds(minutes: int | float) -> int | float:
    return minutes * MINUTES_TO_SECONDS


def seconds_to_minutes(seconds: float, trim: bool = False) -> int | float:
    return seconds // MINUTES_TO_SECONDS if trim else seconds / MINUTES_TO_SECONDS


@overload
def hours_to_nanos(hours: int) -> int: ...


@overload
def hours_to_nanos(hours: float) -> float: ...


def hours_to_nanos(hours: int | float) -> int | float:
    return hours * HOURS_TO_NANOS


def nanos_to_hours(nanos: float, trim: bool = False) -> int | float:
    return nanos // HOURS_TO_NANOS if trim else nanos / HOURS_TO_NANOS


@overload
def hours_to_micros(hours: int) -> int: ...


@overload
def hours_to_micros(hours: float) -> float: ...


def hours_to_micros(hours: int | float) -> int | float:
    return hours * HOURS_TO_MICROS


def micros_to_hours(micros: float, trim: bool = False) -> int | float:
    return micros // HOURS_TO_MICROS if trim else micros / HOURS_TO_MICROS


@overload
def hours_to_millis(hours: int) -> int: ...


@overload
def hours_to_millis(hours: float) -> float: ...


def hours_to_millis(hours: int | float) -> int | float:
    return hours * HOURS_TO_MILLIS


def millis_to_hours(millis: float, trim: bool = False) -> int | float:
    return millis // HOURS_TO_MILLIS if trim else millis / HOURS_TO_MILLIS


@overload
def hours_to_seconds(hours: int) -> int: ...


@overload
def hours_to_seconds(hours: float) -> float: ...


def hours_to_seconds(hours: int | float) -> int | float:
    return hours * HOURS_TO_SECONDS


def seconds_to_hours(seconds: float, trim: bool = False) -> int | float:
    return seconds // HOURS_TO_SECONDS if trim else seconds / HOURS_TO_SECONDS


@overload
def hours_to_minutes(hours: int) -> int: ...


@overload
def hours_to_minutes(hours: float) -> float: ...


def hours_to_minutes(hours: int | float) -> int | float:
    return hours * HOURS_TO_MINUTES


def minutes_to_hours(minutes: float, trim: bool = False) -> int | float:
    return minutes // HOURS_TO_MINUTES if trim else minutes / HOURS_TO_MINUTES


@overload
def days_to_nanos(days: int) -> int: ...


@overload
def days_to_nanos(days: float) -> float: ...


def days_to_nanos(days: int | float) -> int | float:
    return days * DAYS_TO_NANOS


def nanos_to_days(nanos: float, trim: bool = False) -> int | float:
    return nanos // DAYS_TO_NANOS if trim else nanos / DAYS_TO_NANOS


@overload
def days_to_micros(days: int) -> int: ...


@overload
def days_to_micros(days: float) -> float: ...


def days_to_micros(days: int | float) -> int | float:
    return days * DAYS_TO_MICROS


def micros_to_days(micros: float, trim: bool = False) -> int | float:
    return micros // DAYS_TO_MICROS if trim else micros / DAYS_TO_MICROS


@overload
def days_to_millis(days: int) -> int: ...


@overload
def days_to_millis(days: float) -> float: ...


def days_to_millis(days: int | float) -> int | float:
    return days * DAYS_TO_MILLIS


def millis_to_days(millis: float, trim: bool = False) -> int | float:
    return millis // DAYS_TO_MILLIS if trim else millis / DAYS_TO_MILLIS


@overload
def days_to_seconds(days: int) -> int: ...


@overload
def days_to_seconds(days: float) -> float: ...


def days_to_seconds(days: int | float) -> int | float:
    return days * DAYS_TO_SECONDS


def seconds_to_days(seconds: float, trim: bool = False) -> int | float:
    return seconds // DAYS_TO_SECONDS if trim else seconds / DAYS_TO_SECONDS


@overload
def days_to_minutes(days: int) -> int: ...


@overload
def days_to_minutes(days: float) -> float: ...


def days_to_minutes(days: int | float) -> int | float:
    return days * DAYS_TO_MINUTES


def minutes_to_days(minutes: float, trim: bool = False) -> int | float:
    return minutes // DAYS_TO_MINUTES if trim else minutes / DAYS_TO_MINUTES


@overload
def days_to_hours(days: int) -> int: ...


@overload
def days_to_hours(days: float) -> float: ...


def days_to_hours(days: int | float) -> int | float:
    return days * DAYS_TO_HOURS


def hours_to_days(hours: float, trim: bool = False) -> int | float:
    return hours // DAYS_TO_HOURS if trim else hours / DAYS_TO_HOURS