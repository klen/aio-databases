import asyncio

import pytest


async def test_connection(db, aiolib):
    # TODO: Support trio
    if aiolib[0] == 'trio':
        return pytest.skip()

    c1 = db.connection()
    c2 = db.connection(False)
    c3 = db.connection(True)
    assert c1 is c2
    assert c1 is not c3

    async def process():
        async with db.connection() as conn:
            await conn.execute('select 1')
            return conn

    done = await asyncio.gather(process(), process(), process(), process())
    assert done
    assert len(done) == 4
    assert len(set(done)) == 4


async def test_acquire_release(db):
    conn = db.connection()
    await conn.acquire()
    await conn.acquire()
    assert await conn.fetchval('select 42') == 42
    assert conn.is_ready
    await conn.release()
    await conn.release()


async def test_connection_context(db):
    async with db.connection() as conn:
        assert await conn.fetchval('select 1')

    with pytest.raises(ValueError):
        async with db.connection() as conn:
            raise ValueError


@pytest.mark.parametrize('backend', ['aiomysql', 'aiopg', 'asyncpg'])
async def test_pool(pool, aiolib):
    # TODO: Support trio
    if aiolib[0] == 'trio':
        return pytest.skip()

    db = pool

    assert db.backend.pool

    async def process(sql):
        async with db.connection():
            return await db.fetchval(sql)

    done = await asyncio.gather(*[process("select 1") for _ in range(5)])
    assert done
    assert len(done) == 5
    assert done == [1, 1, 1, 1, 1]
