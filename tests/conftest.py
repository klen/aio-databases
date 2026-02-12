import logging
import platform
import sys
from typing import Any

import pytest
from pypika import Dialects
from pypika_orm import Manager, Model, fields

from aio_databases import Database

BACKEND_URLS: dict[str, str] = {
    "aiomysql": "aiomysql://root@127.0.0.1:3306/tests",
    "aiomysql+pool": "aiomysql+pool://root@127.0.0.1:3306/tests",
    "aiopg": "aiopg://test:test@localhost:5432/tests",
    "aiopg+pool": "aiopg+pool://test:test@localhost:5432/tests",
    "asyncpg": "asyncpg://test:test@localhost:5432/tests",
    "asyncpg+pool": "asyncpg+pool://test:test@localhost:5432/tests",
    "aiosqlite": "aiosqlite:////tmp/aio-db-test.sqlite",
    "trio-mysql": "trio-mysql://root@127.0.0.1:3306/tests",
}
BACKEND_PARAMS: dict[str, dict[str, Any]] = {
    "aiomysql": {"autocommit": True},
    "aiomysql+pool": {"maxsize": 2, "autocommit": True},
    "aiopg+pool": {"maxsize": 2},
    "aiosqlite": {"convert_params": True},
    "asyncpg": {"convert_params": True},
    "asyncpg+pool": {"min_size": 2, "max_size": 2, "convert_params": True},
}


@pytest.fixture(
    scope="session",
    params=[
        pytest.param(("asyncio", {"loop_factory": None}), id="asyncio"),
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
def dbparams(backend):
    return BACKEND_PARAMS.get(backend, {})


@pytest.fixture
def db(backend, aiolib, dbparams):

    lib = aiolib[0]

    if lib == "trio" and backend != "trio-mysql":
        return pytest.skip("Trio is only supported by trio-mysql")

    if lib == "trio" and sys.version_info >= (3, 14):
        return pytest.skip("Trio is not supported on Python 3.14 and above")

    if lib == "asyncio" and backend not in {
        "aiomysql",
        "aiomysql+pool",
        "aiopg",
        "aiopg+pool",
        "asyncpg",
        "asyncpg+pool",
        "aiosqlite",
    }:
        return pytest.skip()

    if backend not in BACKEND_PARAMS:
        return pytest.skip()

    url = BACKEND_URLS[backend]
    return Database(url, **dbparams)


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
    mgr = Manager()
    source = db.backend.db_type
    if source == "sqlite":
        source = "sqllite"
    mgr.dialect = Dialects(source)
    return mgr


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

    return platform.processor() == "arm"
