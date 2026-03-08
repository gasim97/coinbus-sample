import select

from dataclasses import dataclass
from logging import Logger
from typing import Optional

from multiprocessing.connection import Connection


MAX_PIPE_PAYLOAD_SIZE_BYTES = 65_536  # 64 KB (non-chunked size: 64 KB)


@dataclass
class SimplexPipe:
    read_connection: Connection
    write_connection: Connection


def is_payload_too_large_for_pipe(payload: bytes) -> bool:
    return len(payload) > MAX_PIPE_PAYLOAD_SIZE_BYTES


def connections_ready_for_read(
    connections: list[Connection], timeout_seconds: int | float = 0.0,
) -> list[Connection]:
    return select.select(connections, [], [], timeout_seconds)[0]


def connections_ready_for_write(
    connections: list[Connection], timeout_seconds: int | float = 0.0,
) -> list[Connection]:
    return select.select([], connections, [], timeout_seconds)[1]


def is_connection_ready_for_read(
    connection: Connection, timeout_seconds: int | float = 0.0,
) -> bool:
    return connection in connections_ready_for_read(
        connections=[connection], timeout_seconds=timeout_seconds,
    )


def is_connection_ready_for_write(
    connection: Connection, timeout_seconds: int | float = 0.0,
) -> bool:
    return connection in connections_ready_for_write(
        connections=[connection], timeout_seconds=timeout_seconds,
    )


def read_pipe_connection(connection: Connection, logger: Logger) -> Optional[bytes]:
    try:
        return connection.recv_bytes()
    except EOFError:
        logger.error("Pipe closed unexpectedly")
        return None
    except Exception:
        logger.error("Pipe read error", exc_info=True)
        return None


def read_pipe_connection_with_readiness_check(
    connection: Connection, logger: Logger, timeout_seconds: int | float = 0.0,
) -> Optional[bytes]:
    try:
        if is_connection_ready_for_read(connection=connection, timeout_seconds=timeout_seconds):
            return connection.recv_bytes()
        return None
    except EOFError:
        logger.error("Pipe closed unexpectedly")
        return None
    except Exception:
        logger.error("Pipe read error", exc_info=True)
        return None


def write_to_pipe_connection(connection: Connection, payload: bytes, logger: Logger) -> bool:
    try:
        connection.send_bytes(payload)
        return True
    except BrokenPipeError:
        logger.error("Pipe write error", exc_info=True)
        return False


def safe_write_to_pipe_connection(connection: Connection, payload: bytes, logger: Logger) -> bool:
    if is_payload_too_large_for_pipe(payload=payload):
        logger.error(f"Payload is too large for pipe. Payload size: {len(payload)} bytes. Max size: {MAX_PIPE_PAYLOAD_SIZE_BYTES} bytes")
        return False
    return write_to_pipe_connection(connection=connection, payload=payload, logger=logger)