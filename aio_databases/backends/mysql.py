from __future__ import annotations

import typing as t
import aiomysql

from . import ABCDabaseBackend, ABCConnectionBackend


class MysqlBackend(ABCDabaseBackend):

    name = 'mysql'
    pool: t.Optional[aiomysql.Pool] = None

    def connection(self) -> MysqlConnection:
        return MysqlConnection(self)

    async def connect(self) -> None:
        self.pool = await aiomysql.create_pool(
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

    async def acquire(self) -> aiomysql.Connection:
        pool = self.pool
        assert pool is not None, "Database is not connected"
        return await pool.acquire()

    async def release(self, conn: aiomysql.Connection):
        pool = self.pool
        assert pool is not None, "Database is not connected"
        await pool.release(conn)


class MysqlConnection(ABCConnectionBackend):

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

    async def fetch(self, query: str, *args, **params) -> t.List[t.Tuple]:
        cursor = await self.conn.cursor()
        try:
            await cursor.execute(query, args)
            res = await cursor.fetchall()
            return list(res)

        finally:
            await cursor.close()

    async def fetchrow(self, query: str, *args, **params) -> t.Optional[t.Tuple]:
        cursor = await self.conn.cursor()
        try:
            await cursor.execute(query, args)
            return await cursor.fetchone()

        finally:
            await cursor.close()

    async def fetchval(self, query: str, *args, column: t.Any = 0, **params) -> t.Any:
        res = await self.fetchrow(query, *args, **params)
        if res:
            res = res[column]
        return res
