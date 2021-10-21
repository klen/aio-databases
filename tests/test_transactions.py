import asyncio

import pytest


async def test_base(db):
    res = None
    async with db.connection():
        async with db.transaction():
            res = await db.fetchval('select 1')

    assert res == 1

    with pytest.raises(RuntimeError):
        async with db.connection(False) as conn:
            async with db.transaction():
                await conn.release()

    async with db.connection(False) as conn:
        async with db.transaction(silent=True):
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


@pytest.mark.parametrize('backend', ['aiopg', 'aiosqlite', 'asyncpg'])
async def test_auto_rollback(db, User, manager):
    UserManager = manager(User)

    await db.execute(UserManager.create_table().if_not_exists())

    with pytest.raises(Exception):
        async with db.transaction() as trans:
            await db.execute(UserManager.insert(name='Jim', fullname='Jim Jones'))
            res = await db.fetchall(UserManager.select())
            assert len(res) == 1
            raise Exception

    res = await db.fetchall(UserManager.select())
    assert len(res) == 0


async def test_nested(pool, User, manager):
    db = pool
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


async def test_connections(db):
    async with db.transaction() as trans1:
        assert trans1

        async with db.transaction() as trans2:
            assert trans2
            assert trans1.connection is trans2.connection

            async with db.transaction(True) as trans3:
                assert trans3
                assert trans3.connection is not trans2.connection


@pytest.mark.skip('not sure the lib has to implement it')
@pytest.mark.parametrize('aiolib', ['asyncio'])
async def test_concurency(db, manager, User):
    import asyncio

    UserManager = manager(User)
    async with db.connection():

        await db.execute(UserManager.create_table().if_not_exists())
        await db.execute(UserManager.insert(name='Tom', fullname='Tom Smith'))

        async def task1():
            async with db.transaction():
                await asyncio.sleep(1e-2)
                qs = UserManager.update().set(User.name, 'Jack').where(User.name == 'Tom')
                await db.execute(qs)

        async def task2():
                qs = UserManager.update().set(User.name, 'Mike').where(User.name == 'Tom')
                await db.execute(qs)

        await asyncio.gather(task1(), task2())

        user = await db.fetchone(UserManager.select())
        assert user['name'] == 'Jack'

        await db.execute(UserManager.drop_table().if_exists())
