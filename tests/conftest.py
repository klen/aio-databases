import logging
from typing import Any, Dict, Tuple

import pytest
from pypika_orm import Manager, Model, fields

BACKEND_PARAMS: Dict[str, Tuple[str, Dict[str, Any]]] = {
    "aiomysql": ("aiomysql://root@127.0.0.1:3306/tests", {"autocommit": True}),
    "aiomysql+pool": (
        "aiomysql+pool://root@127.0.0.1:3306/tests",
        {"maxsize": 2, "autocommit": True},
    ),
    "aiopg": ("aiopg://test:test@localhost:5432/tests", {}),
    "aiopg+pool": ("aiopg+pool://test:test@localhost:5432/tests", {"maxsize": 2}),
    "aiosqlite": ("aiosqlite:////tmp/aio-db-test.sqlite", {"convert_params": True}),
    "asyncpg": ("asyncpg://test:test@localhost:5432/tests", {"convert_params": True}),
    "asyncpg+pool": (
        "asyncpg+pool://test:test@localhost:5432/tests",
        {"min_size": 2, "max_size": 2, "convert_params": True},
    ),
    "trio-mysql": ("trio-mysql://root@127.0.0.1:3306/tests", {}),
    # there is a separate test for triopg
    # Doesnt supports python 3.9
    #  }),
}


@pytest.fixture(
    scope="session",
    params=[
        pytest.param(("asyncio", {"use_uvloop": False}), id="asyncio"),
        "trio",
    ],
)
def aiolib(request):
    """Support asyncio only. Disable uvloop on tests it brokes breakpoints."""
    return request.param


@pytest.fixture(scope="session", params=list(BACKEND_PARAMS))
def backend(request):
    return request.param


@pytest.fixture
def db(backend, aiolib):
    from aio_databases import Database

    if aiolib[0] == "trio" and backend not in {"trio-mysql", "triopg"}:
        return pytest.skip()

    if aiolib[0] == "asyncio" and backend not in {
        "aiomysql",
        "aiomysql+pool",
        "aiopg",
        "aiopg+pool",
        "aiosqlite",
        "asyncpg",
        "asyncpg+pool",
    }:
        return pytest.skip()

    if backend not in BACKEND_PARAMS:
        return pytest.skip()

    url, params = BACKEND_PARAMS[backend]
    return Database(url, **params)


@pytest.fixture(autouse=True)
async def pool(db):
    async with db:
        yield db


@pytest.fixture(scope="session", autouse=True)
def _setup_logging():
    logger = logging.getLogger("aio-databases")
    logger.setLevel(logging.DEBUG)


@pytest.fixture
def manager(db):
    return Manager(dialect=db.backend.db_type)


@pytest.fixture
def user_cls():
    class User(Model):
        id = fields.Auto()
        name = fields.Varchar()
        fullname = fields.Varchar()

    return User


@pytest.fixture
def comment_cls(user_cls):
    class Comment(Model):
        id = fields.Auto()
        body = fields.Varchar()

        user_id = fields.ForeignKey(user_cls.id)

    return Comment


@pytest.fixture(scope="session")
def arm():
    import platform

    return platform.processor() == "arm"
