import asyncio

import pytest


async def test_base(db):
    with pytest.raises(RuntimeError):
        async with db.transaction():
            assert await db.fetchval('select 1')

    res = None
    async with db.connection():
        async with db.transaction():
            res = await db.fetchval('select 1')

    assert res == 1


async def test_child_tasks(db):
    res = await db.fetchval('select 1')
    assert res == 1

    async def process():
        async with db.connection():
            async with db.transaction():
                return await db.fetchval('select 1')

    res = await asyncio.gather(process(), process(), process(), process())
    assert res == [1, 1, 1, 1]
