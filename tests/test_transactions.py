import asyncio

import pytest


async def test_base(db):
    # Close current connection
    async with db.connection(False):
        pass

    with pytest.raises(RuntimeError):
        async with db.transaction():
            assert await db.fetchval('select 1')

    res = None
    async with db.connection():
        async with db.transaction():
            res = await db.fetchval('select 1')

    assert res == 1

    with pytest.raises(RuntimeError):
        async with db.connection(False) as conn:
            async with db.transaction():
                await conn.release()


async def test_child_tasks(db, aiolib):
    if aiolib[0] == 'trio':
        return pytest.skip()

    res = await db.fetchval('select 1')
    assert res == 1

    async def process():
        async with db.connection():
            async with db.transaction():
                return await db.fetchval('select 1')

    res = await asyncio.gather(process(), process(), process(), process())
    assert res == [1, 1, 1, 1]


async def test_nested(db, User, manager):
    UserManager = manager(User)
    await db.execute(UserManager.create_table().if_not_exists())
    await db.execute(UserManager.delete())

    async with db.transaction() as main_trans:
        assert main_trans

        res = await db.execute(UserManager.insert(name='Jim', fullname='Jim Jones'))
        assert res

        res = await db.fetchall(UserManager.select())
        assert res
        assert len(res) == 1

        async with db.transaction() as trans2:
            assert trans2

            res = await db.execute(UserManager.insert(name='Tom', fullname='Tom Smith'))
            assert res

            res = await db.fetchall(UserManager.select())
            assert res
            assert len(res) == 2

            async with db.transaction() as trans3:
                assert trans3

                res = await db.execute(UserManager.insert(name='Jerry', fullname='Jerry Mitchel'))
                assert res

                res = await db.fetchall(UserManager.select())
                assert res
                assert len(res) == 3

                await trans3.rollback()

            res = await db.fetchall(UserManager.select())
            assert res
            assert len(res) == 2

            await trans2.rollback()

        res = await db.fetchall(UserManager.select())
        assert res
        assert len(res) == 1

        await main_trans.rollback()

    res = await db.fetchall(UserManager.select())
    assert not res

    await db.execute(UserManager.drop_table().if_exists())
