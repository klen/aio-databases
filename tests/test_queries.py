import pytest
from pypika import Parameter


@pytest.fixture(autouse=True)
async def schema(db, User, manager):
    UserManager = manager(User)

    await db.execute(UserManager.create_table().if_not_exists())
    yield
    await db.execute(UserManager.drop_table().if_exists())


async def test_base(db):
    await db.execute("select %s", '1')

    res = await db.fetchall("select (2 * %s) res", 2)
    assert [tuple(r) for r in res] == [(4,)]

    res = await db.fetchmany(10, "select (2 * %s) res", 2)
    assert [tuple(r) for r in res] == [(4,)]

    res = await db.fetchone("select (2 * %s) res", 2)
    assert tuple(res) == (4,)

    res = await db.fetchval("select 2 + %s", 2)
    assert res == 4


async def test_all(db, User, manager, schema):
    UserManager = manager(User)
    await db.execute(UserManager.delete())

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

    res = await db.fetchone(UserManager.select().where(User.id == 100))
    assert res is None


async def test_execute_many(db, User, manager, schema):
    UserManager = manager(User)
    await db.execute(UserManager.delete())
    qs = UserManager.insert(name=Parameter('%s'), fullname=Parameter('%s'))
    await db.executemany(qs, ('Jim', 'Jim Jones'), ('Tom', 'Tom Smith'))
    res = await db.fetchall(UserManager.select())
    assert res
    assert len(res) == 2
    u1, u2 = res
    assert u1['name'] == 'Jim'
    assert u2['name'] == 'Tom'


async def test_iterate(db, User, manager, schema):
    UserManager = manager(User)
    qs = UserManager.insert(name=Parameter('%s'), fullname=Parameter('%s'))
    await db.executemany(qs, ('Jim', 'Jim Jones'), ('Tom', 'Tom Smith'))

    async for rec in db.iterate(UserManager.select()):
        assert rec['name'] in {'Jim', 'Tom'}
