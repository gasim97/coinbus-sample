from datetime import datetime, timezone

from common.utils.time import (
    micros_to_nanos,
    micros_to_seconds,
    millis_to_nanos,
    millis_to_seconds,
    nanos_to_seconds,
    seconds_to_micros,
    seconds_to_millis,
    seconds_to_nanos,
)
from generated.type.common.constants import SECONDS_TO_NANOS


def nanos_timestamp_to_date(timestamp: int | float, tz: timezone = timezone.utc) -> str:
    return seconds_timestamp_to_date(timestamp=nanos_to_seconds(timestamp), tz=tz)


def micros_timestamp_to_date(timestamp: int | float, tz: timezone = timezone.utc) -> str:
    return seconds_timestamp_to_date(timestamp=micros_to_seconds(timestamp), tz=tz)


def millis_timestamp_to_date(timestamp: int | float, tz: timezone = timezone.utc) -> str:
    return seconds_timestamp_to_date(timestamp=millis_to_seconds(timestamp), tz=tz)


def seconds_timestamp_to_date(timestamp: int | float, tz: timezone = timezone.utc) -> str:
    return datetime.fromtimestamp(timestamp=timestamp, tz=tz).strftime('%Y%m%d')


def nanos_timestamp_to_time(timestamp: int | float, tz: timezone = timezone.utc) -> str:
    nanos = str(int(timestamp % SECONDS_TO_NANOS)).zfill(9)
    return f"{datetime.fromtimestamp(timestamp=nanos_to_seconds(timestamp, trim=True), tz=tz).strftime('%H:%M:%S')}.{nanos}"


def micros_timestamp_to_time(timestamp: int | float, tz: timezone = timezone.utc) -> str:
    return nanos_timestamp_to_time(timestamp=micros_to_nanos(timestamp), tz=tz)[:-3]


def millis_timestamp_to_time(timestamp: int | float, tz: timezone = timezone.utc) -> str:
    return nanos_timestamp_to_time(timestamp=millis_to_nanos(timestamp), tz=tz)[:-6]


def seconds_timestamp_to_time(timestamp: int | float, tz: timezone = timezone.utc) -> str:
    return nanos_timestamp_to_time(timestamp=seconds_to_nanos(timestamp), tz=tz)[:-10]


def nanos_time_to_timestamp(time: str, now_timestamp: int | float, tz: timezone = timezone.utc) -> float:
    """Convert a time string in format 'HH:MM:SS.nnnnnnnnn' to nanoseconds timestamp"""
    base_time, nanos = time.split('.')
    seconds = seconds_time_to_timestamp(base_time, nanos_to_seconds(now_timestamp, trim=True), tz)
    return seconds_to_nanos(seconds) + float(nanos)


def micros_time_to_timestamp(time: str, now_timestamp: int | float, tz: timezone = timezone.utc) -> float:
    """Convert a time string in format 'HH:MM:SS.uuuuuu' to microseconds timestamp"""
    base_time, micros = time.split('.')
    seconds = seconds_time_to_timestamp(base_time, micros_to_seconds(now_timestamp, trim=True), tz)
    return seconds_to_micros(seconds) + float(micros)


def millis_time_to_timestamp(time: str, now_timestamp: int | float, tz: timezone = timezone.utc) -> float:
    """Convert a time string in format 'HH:MM:SS.mmm' to milliseconds timestamp"""
    base_time, millis = time.split('.')
    seconds = seconds_time_to_timestamp(base_time, millis_to_seconds(now_timestamp, trim=True), tz)
    return seconds_to_millis(seconds) + float(millis)


def seconds_time_to_timestamp(time: str, now_timestamp: int | float, tz: timezone = timezone.utc) -> float:
    """Convert a time string in format 'HH:MM:SS' to seconds timestamp"""
    today = datetime.fromtimestamp(now_timestamp, tz).strftime('%Y-%m-%d')
    dt = datetime.strptime(f"{today} {time}", '%Y-%m-%d %H:%M:%S').replace(tzinfo=tz)
    return dt.timestamp()


def nanos_timestamp_to_datetime(timestamp: int | float, tz: timezone = timezone.utc) -> str:
    nanos = str(int(timestamp % SECONDS_TO_NANOS)).zfill(9)
    return f"{datetime.fromtimestamp(timestamp=nanos_to_seconds(timestamp, trim=True), tz=tz).strftime('%Y%m%d-%H.%M.%S')}.{nanos}"


def micros_timestamp_to_datetime(timestamp: int | float, tz: timezone = timezone.utc) -> str:
    return nanos_timestamp_to_datetime(timestamp=micros_to_nanos(timestamp), tz=tz)[:-3]


def millis_timestamp_to_datetime(timestamp: int | float, tz: timezone = timezone.utc) -> str:
    return nanos_timestamp_to_datetime(timestamp=millis_to_nanos(timestamp), tz=tz)[:-6]


def seconds_timestamp_to_datetime(timestamp: int | float, tz: timezone = timezone.utc) -> str:
    return nanos_timestamp_to_datetime(timestamp=seconds_to_nanos(timestamp), tz=tz)[:-10]