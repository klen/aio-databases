import typing as t

from uuid import uuid4

from . import ABCTransaction, ABCConnection
from ..record import Record


class Transaction(ABCTransaction):

    savepoint: t.Optional[str] = None

    async def _start(self) -> t.Any:
        connection = self.connection
        sql = 'BEGIN'
        if connection.transactions:
            self.savepoint = savepoint = f"AIODB_SAVEPOINT_{uuid4().hex}"
            sql = f"SAVEPOINT {savepoint}"
        connection.logger.debug((sql,))
        return await connection._execute(sql)

    async def _commit(self) -> t.Any:
        savepoint = self.savepoint
        connection = self.connection
        sql = 'COMMIT'
        if savepoint:
            sql = f"RELEASE SAVEPOINT {savepoint}"
        connection.logger.debug((sql,))
        return await connection._execute(sql)

    async def _rollback(self) -> t.Any:
        savepoint = self.savepoint
        connection = self.connection
        sql = 'ROLLBACK'
        if savepoint:
            sql = f"ROLLBACK TO SAVEPOINT {savepoint}"
        connection.logger.debug((sql,))
        return await connection._execute(sql)


class Connection(ABCConnection):

    transaction_cls = Transaction

    async def _execute(self, query: str, *params, **options) -> t.Any:
        async with self.conn.cursor() as cursor:
            await cursor.execute(query, params, **options)
            if cursor.lastrowid == 0:
                return cursor.rowcount
            return cursor.lastrowid

    async def _executemany(self, query: str, *params, **options) -> t.Any:
        async with self.conn.cursor() as cursor:
            await cursor.executemany(query, params, **options)

    async def _fetchall(self, query: str, *params, **options) -> t.List[t.Mapping]:
        async with self.conn.cursor() as cursor:
            await cursor.execute(query, params, **options)
            rows = await cursor.fetchall()
            desc = cursor.description
            return [Record(row, desc) for row in rows]

    async def _fetchmany(self, size: int, query: str, *params, **options) -> t.List[t.Mapping]:
        async with self.conn.cursor() as cursor:
            await cursor.execute(query, params, **options)
            rows = await cursor.fetchmany(size)
            desc = cursor.description
            return [Record(row, desc) for row in rows]

    async def _fetchone(self, query: str, *params, **options) -> t.Optional[t.Mapping]:
        async with self.conn.cursor() as cursor:
            await cursor.execute(query, params, **options)
            row = await cursor.fetchone()
            if row is None:
                return row
            return Record(row, cursor.description)

    async def _fetchval(self, query: str, *params, column: t.Any = 0, **options) -> t.Any:
        async with self.conn.cursor() as cursor:
            await cursor.execute(query, params, **options)
            row = await cursor.fetchone()
            if row is None:
                return row
            return row[column]

    async def _iterate(self, query: str, *params, **options) -> t.AsyncIterator[Record]:
        async with self.conn.cursor() as cursor:
            await cursor.execute(query, params, **options)
            desc = cursor.description
            while True:
                row = await cursor.fetchone()
                if row is None:
                    break
                yield Record(row, desc)
