from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional

from . import ABCConnection, ABCDatabaseBackend
from .common import Transaction

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from aio_databases.types import TRecord


class Connection(ABCConnection):
    transaction_cls = Transaction

    async def _execute(self, query: str, *params, **options) -> Any:
        return None

    async def _executemany(self, query: str, *params, **options) -> Any:
        return None

    async def _fetchall(self, query: str, *params, **options) -> list[TRecord]:
        return []

    async def _fetchmany(self, size: int, query: str, *params, **options) -> list[TRecord]:
        return []

    async def _fetchone(self, query: str, *params, **options) -> Optional[TRecord]:
        return None

    async def _fetchval(self, query: str, *params, column: Any = 0, **options) -> Any:
        return None

    async def _iterate(self, query: str, *params, **options) -> AsyncIterator[TRecord]:
        yield {}


class Backend(ABCDatabaseBackend):
    """Must not be used in production."""

    name = "dummy"
    db_type = "dummy"
    connection_cls = Connection

    async def _acquire(self):
        pass

    async def release(self, conn):
        pass
