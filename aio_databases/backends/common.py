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
            self.savepoint = savepoint = f"AIODB__{uuid4().hex}"
            sql = f"SAVEPOINT {savepoint}"
        return await connection.execute(sql)

    async def _commit(self) -> t.Any:
        savepoint = self.savepoint
        connection = self.connection
        sql = 'COMMIT'
        if savepoint:
            sql = f"RELEASE SAVEPOINT {savepoint}"
        return await connection.execute(sql)

    async def _rollback(self) -> t.Any:
        savepoint = self.savepoint
        connection = self.connection
        sql = 'ROLLBACK'
        if savepoint:
            sql = f"ROLLBACK TO SAVEPOINT {savepoint}"
        return await connection.execute(sql)


class Connection(ABCConnection):

    transaction_cls = Transaction

    async def _execute(self, query: str, *params, **options) -> t.Tuple[int, t.Any]:
        async with self._conn.cursor() as cursor:
            await cursor.execute(query, params, **options)
            return cursor.rowcount, cursor.lastrowid

    async def _executemany(self, query: str, *params, **options) -> t.Any:
        async with self._conn.cursor() as cursor:
            await cursor.executemany(query, params, **options)

    async def _fetchall(self, query: str, *params, **options) -> t.List[t.Mapping]:
        async with self._conn.cursor() as cursor:
            await cursor.execute(query, params, **options)
            rows = await cursor.fetchall()
            desc = cursor.description
            return [Record(row, desc) for row in rows]

    async def _fetchmany(self, size: int, query: str, *params, **options) -> t.List[t.Mapping]:
        async with self._conn.cursor() as cursor:
            await cursor.execute(query, params, **options)
            rows = await cursor.fetchmany(size)
            desc = cursor.description
            return [Record(row, desc) for row in rows]

    async def _fetchone(self, query: str, *params, **options) -> t.Optional[t.Mapping]:
        async with self._conn.cursor() as cursor:
            await cursor.execute(query, params, **options)
            row = await cursor.fetchone()
            if row is None:
                return row
            return Record(row, cursor.description)

    async def _fetchval(self, query: str, *params, column: t.Any = 0, **options) -> t.Any:
        async with self._conn.cursor() as cursor:
            await cursor.execute(query, params, **options)
            row = await cursor.fetchone()
            if row is None:
                return row
            return row[column]

    async def _iterate(self, query: str, *params, **options) -> t.AsyncIterator[Record]:
        async with self._conn.cursor() as cursor:
            await cursor.execute(query, params, **options)
            desc = cursor.description
            while True:
                row = await cursor.fetchone()
                if row is None:
                    break
                yield Record(row, desc)


class PGReplacer:

    __slots__ = ('num',)

    def __init__(self):
        self.num = 0

    def __call__(self, match):
        self.num += 1
        return f"{match.group(1)}${self.num}"


def pg_parse_status(status: str) -> t.Union[str, t.Tuple[int, t.Any]]:
    operation, params = status.split(' ', 1)
    if operation in {'INSERT'}:
        oid, rows = params.split()
        return int(rows), oid

    if operation in {'UPDATE', 'DELETE'}:
        return int(params.split()[0]), None

    return status
