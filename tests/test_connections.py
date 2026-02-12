from __future__ import annotations

import asyncio

import pytest
import trio

from aio_databases import Database


async def test_default_connection(db: Database):
    assert int(await db.fetchval("select 42")) == 42


async def test_db_context(db):

    database = Database("dummy://")
    database.backend = db.backend
    async with database, database.connection() as conn:
        await conn.execute("select 1")
        assert database.current_conn is not None
        assert database.current_conn is conn
        assert database.current_conn.is_ready

    assert database.current_conn is None


async def test_connection_context(db: Database):
    assert db.current_conn is None

    assert await db.fetchval("select 42") == 42

    # acquire a new connection
    async with db.connection() as c1:
        assert c1
        assert c1 is db.current_conn

        # acquire a new connection
        async with db.connection() as c2:
            assert c2 is not c1
            assert c2 is db.current_conn

            # use if exist
            async with db.connection(create=False) as c3:
                assert c3 is c2
                assert c3 is db.current_conn

            assert c2 is db.current_conn

        assert c1 is db.current_conn

    assert db.current_conn is None


async def test_multiconnections(db: Database, aiolib: str):
    done = []

    async def process():
        async with db.connection() as conn:
            await conn.execute("select 1")
            done.append(conn)

    if aiolib[0] == "trio":
        async with trio.open_nursery() as tasks:
            tasks.start_soon(process)
            tasks.start_soon(process)
            tasks.start_soon(process)
            tasks.start_soon(process)

    else:
        await asyncio.gather(process(), process(), process(), process())

    assert len(done) == 4
    assert len(set(done)) == 4


async def test_double_acquire_release(db: Database):
    async with db.connection() as conn:
        await conn.acquire()
        await conn.acquire()
        assert await conn.fetchval("select 42") == 42
        assert conn.is_ready
        await conn.release()
        await conn.release()


async def test_connection_context_internal_exceptions(db: Database):
    async with db.connection() as conn:
        assert await conn.fetchval("select 1")

    with pytest.raises(ValueError, match="test"):
        async with db.connection() as conn:
            raise ValueError("test")


@pytest.mark.parametrize("backend", ["aiomysql+pool", "aiopg+pool", "asyncpg+pool"])
async def test_pool(pool: Database, aiolib: str):
    db = pool

    assert db.backend.pool

    async def process(sql):
        async with db.connection():
            return await db.fetchval(sql)

    done = await asyncio.gather(*[process("select 1") for _ in range(5)])
    assert done
    assert len(done) == 5
    assert done == [1, 1, 1, 1, 1]
