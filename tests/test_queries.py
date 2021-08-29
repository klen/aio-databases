import pytest


@pytest.fixture(autouse=True)
async def schema(db, User, manager):
    UserManager = manager(User)

    await db.execute(UserManager.create_table().if_not_exists())
    yield
    await db.execute(UserManager.drop_table().if_exists())


async def test_base(db, param):
    ph = param()
    await db.execute(f"select {ph}", '1')

    res = await db.fetchall(f"select (2 * {ph}) res", 2)
    assert res == [(4,)]

    res = await db.fetchone(f"select (2 * {ph}) res", 2)
    assert res == (4,)
    assert isinstance(res, db.backend.record_cls)

    res = await db.fetchval(f"select 2 + {ph}", 2)
    assert res == 4


async def test_all(db, User, manager, schema):
    UserManager = manager(User)

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
    assert user['id'] == 1
    assert user['name'] == 'Jim'
    assert user['fullname'] == 'Jim Jones'

    res = await db.fetchone(UserManager.select().where(User.id == 100))
    assert res is None


async def test_execute_many(db, User, manager, schema, param):
    from pypika import Parameter

    UserManager = manager(User)
    qs = UserManager.insert(name=Parameter(param()), fullname=Parameter(param()))
    await db.executemany(qs, ('Jim', 'Jim Jones'), ('Tom', 'Tom Smith'))
    res = await db.fetchall(UserManager.select())
    assert res
    assert len(res) == 2
    u1, u2 = res
    assert u1['name'] == 'Jim'
    assert u2['name'] == 'Tom'
