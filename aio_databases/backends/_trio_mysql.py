from __future__ import annotations

import trio
import trio_mysql

from . import ABCDatabaseBackend
from .common import Connection as Connection_


class Connection(Connection_):

    lock_cls = trio.Lock


class Backend(ABCDatabaseBackend):

    name = 'trio-mysql'
    db_type = 'mysql'
    connection_cls = Connection

    def __init__(self, *args, autocommit=True, charset='utf8', use_unicode=True, **options):
        """Setup default value for autocommit."""
        super(Backend, self).__init__(
            *args, autocommit=autocommit, charset=charset, use_unicode=use_unicode, **options)

    async def connect(self) -> None:
        pass

    async def disconnect(self) -> None:
        pass

    async def acquire(self) -> trio_mysql.Connection:
        conn = trio_mysql.connect(
            **self.options,
            host=self.url.hostname,
            port=self.url.port,
            user=self.url.username,
            password=self.url.password,
            db=self.url.path.strip('/'),
        )
        await conn.connect()
        return conn

    async def release(self, conn: trio_mysql.Connection):
        conn.close()
