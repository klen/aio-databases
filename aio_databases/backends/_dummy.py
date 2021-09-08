import typing as t

from . import ABCDatabaseBackend, ABCConnection, ABCTransaction


class Transaction(ABCTransaction):

    async def _start(self) -> t.Any:
        return None

    async def _commit(self) -> t.Any:
        return None

    async def _rollback(self) -> t.Any:
        return None


class Connection(ABCConnection):

    transaction_cls = Transaction

    async def _execute(self, query: str, *params, **options) -> t.Any:
        return None

    async def _executemany(self, query: str, *params, **options) -> t.Any:
        return None

    async def _fetchall(self, query: str, *params, **options) -> t.List[t.Mapping]:
        return []

    async def _fetchmany(self, size: int, query: str, *params, **options) -> t.List[t.Mapping]:
        return []

    async def _fetchone(self, query: str, *params, **options) -> t.Optional[t.Mapping]:
        return None

    async def _fetchval(self, query: str, *params, column: t.Any = 0, **options) -> t.Any:
        return None

    async def _iterate(self, query: str, *params, **options) -> t.AsyncIterator:
        yield None


class Backend(ABCDatabaseBackend):
    """Must not be used in production."""

    name = 'dummy'
    db_type = 'dummy'
    connection_cls = Connection

    async def connect(self):
        pass

    async def disconnect(self):
        pass

    async def acquire(self):
        pass

    async def release(self, conn):
        pass
