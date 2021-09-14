import pytest
import trio_asyncio
from pypika import Parameter
from pypika_orm import Manager


@pytest.fixture
def aiolib():
    return 'trio'


async def test_base(User):
    from aio_databases import Database

    async with trio_asyncio.open_loop():
        async with Database('triopg://test:test@localhost:5432/tests', convert_params=True) as db:
            manager = Manager(db)
            UserManager = manager(User)

            await db.execute('select %s', '1')

            res = await db.fetchall("select (2 * %s) res", 2)
            assert [tuple(r) for r in res] == [(4,)]

            res = await db.fetchmany(10, "select (2 * %s) res", 2)
            assert [tuple(r) for r in res] == [(4,)]

            res = await db.fetchone("select (2 * %s) res", 2)
            assert tuple(res) == (4,)

            res = await db.fetchval("select 2 + %s", 2)
            assert res == 4

            await db.execute(UserManager.drop_table().if_exists())
            await db.execute(UserManager.create_table().if_not_exists())

            async with db.transaction() as main_trans:
                assert main_trans

                res = await db.execute(UserManager.insert(name='Jim', fullname='Jim Jones'))
                assert res

                async with db.transaction() as trans2:
                    assert trans2

                    res = await db.execute(UserManager.insert(name='Tom', fullname='Tom Smith'))
                    assert res

                    res = await db.fetchall(UserManager.select())
                    assert res
                    assert len(res) == 2

                    await trans2.rollback()

            res = await db.fetchall(UserManager.select())
            assert res
            assert len(res) == 1

            [user] = res
            assert user
            assert user['id']
            assert user['name'] == 'Jim'
            assert user['fullname'] == 'Jim Jones'

            res = await db.execute(UserManager.update().set(User.name, 'Newbie'))
            assert res == (1, None)

            res = await db.fetchone(UserManager.select().where(User.id == 100))
            assert res is None

            await db.execute(UserManager.delete())
            qs = UserManager.insert(name=Parameter('$1'), fullname=Parameter('$2'))
            await db.executemany(qs, ('Jim', 'Jim Jones'), ('Tom', 'Tom Smith'))
            res = await db.fetchall(UserManager.select())
            assert res
            assert len(res) == 2
            u1, u2 = res
            assert u1['name'] == 'Jim'
            assert u2['name'] == 'Tom'

            async for rec in db.iterate(UserManager.select()):
                assert rec['name'] in {'Jim', 'Tom'}

            await db.execute(UserManager.drop_table().if_exists())
