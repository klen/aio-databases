from __future__ import annotations

from typing import TYPE_CHECKING, Any
from uuid import uuid4

from aio_databases.record import Record
from aio_databases.types import TVConnection

from . import ABCConnection, ABCTransaction

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from aio_databases.types import TRecord


class Transaction(ABCTransaction):
    savepoint: str | None = None

    async def _start(self) -> Any:
        connection = self.connection
        sql = "BEGIN"
        if connection.transactions:
            self.savepoint = savepoint = f"AIODB__{uuid4().hex}"
            sql = f"SAVEPOINT {savepoint}"
        return await connection.execute(sql)

    async def _commit(self) -> Any:
        savepoint = self.savepoint
        connection = self.connection
        sql = "COMMIT"
        if savepoint:
            sql = f"RELEASE SAVEPOINT {savepoint}"
        return await connection.execute(sql)

    async def _rollback(self) -> Any:
        savepoint = self.savepoint
        connection = self.connection
        sql = "ROLLBACK"
        if savepoint:
            sql = f"ROLLBACK TO SAVEPOINT {savepoint}"
        return await connection.execute(sql)


class Connection(ABCConnection[TVConnection]):
    transaction_cls = Transaction

    async def _execute(self, query: str, *params, **options) -> tuple[int, Any]:
        conn = self._conn
        assert conn is not None
        async with conn.cursor() as cursor:  # type: ignore[missing-attribute]
            await cursor.execute(query, params, **options)
            return cursor.rowcount, cursor.lastrowid

    async def _executemany(self, query: str, *params, **options) -> Any:
        conn = self._conn
        assert conn is not None
        async with conn.cursor() as cursor:  # type: ignore[missing-attribute]
            await cursor.executemany(query, params, **options)

    async def _fetchall(self, query: str, *params, **options) -> list[TRecord]:
        conn = self._conn
        assert conn is not None
        async with conn.cursor() as cursor:  # type: ignore[missing-attribute]
            await cursor.execute(query, params, **options)
            rows = await cursor.fetchall()
            desc = cursor.description
            return [Record(row, desc) for row in rows]

    async def _fetchmany(self, size: int, query: str, *params, **options) -> list[TRecord]:
        conn = self._conn
        assert conn is not None
        async with conn.cursor() as cursor:  # type: ignore[missing-attribute]
            await cursor.execute(query, params, **options)
            rows = await cursor.fetchmany(size)
            desc = cursor.description
            return [Record(row, desc) for row in rows]

    async def _fetchone(self, query: str, *params, **options) -> TRecord | None:
        conn = self._conn
        assert conn is not None
        async with conn.cursor() as cursor:  # type: ignore[missing-attribute]
            await cursor.execute(query, params, **options)
            row = await cursor.fetchone()
            if row is None:
                return row
            return Record(row, cursor.description)

    async def _fetchval(self, query: str, *params, column: Any = 0, **options) -> Any:
        conn = self._conn
        assert conn is not None
        async with conn.cursor() as cursor:  # type: ignore[missing-attribute]
            await cursor.execute(query, params, **options)
            row = await cursor.fetchone()
            if row is None:
                return row
            return row[column]

    async def _iterate(self, query: str, *params, **options) -> AsyncIterator[Record]:
        conn = self._conn
        assert conn is not None
        async with conn.cursor() as cursor:  # type: ignore[missing-attribute]
            await cursor.execute(query, params, **options)
            desc = cursor.description
            while True:
                row = await cursor.fetchone()
                if row is None:
                    break
                yield Record(row, desc)


class PGReplacer:
    __slots__ = ("num",)

    def __init__(self):
        self.num = 0

    def __call__(self, match):
        self.num += 1
        return f"{match.group(1)}${self.num}"


def pg_parse_status(status: str) -> str | tuple[int, Any]:
    operation, params = status.split(" ", 1)
    if operation == "INSERT":
        oid, rows = params.split()
        return int(rows), oid

    if operation in {"UPDATE", "DELETE"}:
        return int(params.split()[0]), None

    return status
