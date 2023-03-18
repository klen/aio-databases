from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest
from pypika import Parameter

if TYPE_CHECKING:
    from pypika_orm import Manager, Model

    from aio_databases import Database


@pytest.fixture()
async def schema(pool: Database, user_cls: Model, manager: Manager):
    user_manager = manager(user_cls)

    await pool.execute(user_manager.create_table().if_not_exists())
    yield True
    await pool.execute(user_manager.drop_table().if_exists())


async def test_base(db: Database):
    await db.execute("select %s", "1")

    res: Any = await db.fetchall("select (2 * %s) res", 2)
    assert [tuple(r) for r in res] == [(4,)]

    res = await db.fetchmany(10, "select (2 * %s) res", 2)
    assert [tuple(r) for r in res] == [(4,)]

    res = await db.fetchone("select (2 * %s) res", 2)
    assert tuple(res) == (4,)

    res = await db.fetchval("select 2 + %s", 2)
    assert res == 4


async def test_all(db: Database, user_cls: Model, manager: Manager, schema):
    user_manager = manager(user_cls)
    await db.execute(user_manager.delete())

    async with db.transaction() as main_trans:
        assert main_trans

        res = await db.execute(user_manager.insert(name="Jim", fullname="Jim Jones"))
        assert res

        async with db.transaction() as trans2:
            assert trans2

            res = await db.execute(user_manager.insert(name="Tom", fullname="Tom Smith"))
            assert res

            res = await db.fetchall(user_manager.select())
            assert res
            assert len(res) == 2

            await trans2.rollback()

    res = await db.fetchall(user_manager.select())
    assert res
    assert len(res) == 1

    [user] = res
    assert user
    assert user["id"]
    assert user["name"] == "Jim"
    assert user["fullname"] == "Jim Jones"

    res = await db.fetchone(user_manager.select().where(user_cls.id == 100))
    assert res is None


async def test_execute(db: Database, user_cls: Model, manager: Manager, schema):
    user_manager = manager(user_cls)
    await db.execute(user_manager.insert(name="Jim", fullname="Tom Smith"))
    await db.execute(user_manager.insert(name="Jim", fullname="Tom Smith"))
    updated, lastid = await db.execute(user_manager.update().set(user_cls.name, "Tom"))
    assert updated == 2


@pytest.mark.parametrize("backend", ["aiomysql"])
async def test_execute_many(db: Database, user_cls: Model, manager: Manager, schema):
    user_manager = manager(user_cls)
    await db.execute(user_manager.delete())
    qs = user_manager.insert(name=Parameter("%s"), fullname=Parameter("%s"))
    await db.executemany(qs, ("Jim", "Jim Jones"), ("Tom", "Tom Smith"))
    res = await db.fetchall(user_manager.select())
    assert res
    assert len(res) == 2
    u1, u2 = res
    assert u1["name"] == "Jim"
    assert u2["name"] == "Tom"


async def test_iterate(db: Database, user_cls: Model, manager: Manager, schema):
    user_manager = manager(user_cls)
    qs = user_manager.insert(name=Parameter("%s"), fullname=Parameter("%s"))
    await db.executemany(qs, ("Jim", "Jim Jones"), ("Tom", "Tom Smith"))

    async for rec in db.iterate(user_manager.select()):
        assert rec["name"] in {"Jim", "Tom"}
