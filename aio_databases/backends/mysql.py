from __future__ import annotations

import typing as t
from uuid import uuid4

from aiomysql import Pool, Connection, create_pool

from . import ABCDabaseBackend, ABCConnection, ABCTransaction
from ..record import Record


class MysqlBackend(ABCDabaseBackend):

    name = 'mysql'
    pool: t.Optional[Pool] = None

    def connection(self) -> MysqlConnection:
        return MysqlConnection(self)

    async def connect(self) -> None:
        self.pool = await create_pool(
            **self.options,
            host=self.url.hostname,
            port=self.url.port,
            user=self.url.username,
            password=self.url.password,
            db=self.url.path.strip('/'),
            autocommit=True,
        )

    async def disconnect(self) -> None:
        pool = self.pool
        assert pool is not None, "Database is not connected"
        self.pool = None
        pool.close()
        await pool.wait_closed()

    async def acquire(self) -> Connection:
        pool = self.pool
        assert pool is not None, "Database is not connected"
        return await pool.acquire()

    async def release(self, conn: Connection):
        pool = self.pool
        assert pool is not None, "Database is not connected"
        await pool.release(conn)


class MysqlConnection(ABCConnection):

    async def execute(self, query: str, *args, **params) -> t.Any:
        cursor = await self.conn.cursor()
        try:
            await cursor.execute(query, args)
            if cursor.lastrowid == 0:
                return cursor.rowcount
            return cursor.lastrowid

        finally:
            await cursor.close()

    async def executemany(self, query: str, *args, **params) -> t.Any:
        cursor = await self.conn.cursor()
        try:
            for args_ in args:
                await cursor.execute(query, args_)

        finally:
            await cursor.close()

    async def fetchall(self, query: str, *args, **params) -> t.List[t.Mapping]:
        cursor = await self.conn.cursor()
        try:
            await cursor.execute(query, args)
            rows = await cursor.fetchall()
            desc = cursor.description
            return [Record(row, desc) for row in rows]

        finally:
            await cursor.close()

    async def fetchone(self, query: str, *args, **params) -> t.Optional[t.Mapping]:
        cursor = await self.conn.cursor()
        try:
            await cursor.execute(query, args)
            row = await cursor.fetchone()
            if row is None:
                return row

            return Record(row, cursor.description)

        finally:
            await cursor.close()

    async def fetchval(self, query: str, *args, column: t.Any = 0, **params) -> t.Any:
        res = await self.fetchone(query, *args, **params)
        if res:
            res = res[column]
        return res

    def transaction(self) -> MysqlTransaction:
        return MysqlTransaction(self)


class MysqlTransaction(ABCTransaction):

    savepoint: t.Optional[str] = None

    async def _start(self) -> t.Any:
        connection = self.connection
        if connection.transactions:
            self.savepoint = savepoint = f"AIODB_SAVEPOINT_{uuid4().hex}"
            return await connection.execute(f"SAVEPOINT {savepoint}")

        return await connection.conn.begin()

    async def _commit(self) -> t.Any:
        savepoint = self.savepoint
        if savepoint:
            return await self.connection.execute(f"RELEASE SAVEPOINT {savepoint}")

        return await self.connection.conn.commit()

    async def _rollback(self) -> t.Any:
        savepoint = self.savepoint
        if savepoint:
            return await self.connection.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")

        return await self.connection.conn.rollback()
