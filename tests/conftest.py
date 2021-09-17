import pytest
import logging

from pypika_orm import Manager, Model, fields


BACKEND_PARAMS = {
    'aiomysql': ('aiomysql://root@127.0.0.1:3306/tests', {'maxsize': 2}),
    'aiopg': ('aiopg://test:test@localhost:5432/tests', {'maxsize': 2}),
    'aiosqlite': ('aiosqlite:///:memory:', {'convert_params': True}),
    'asyncpg': ('asyncpg://test:test@localhost:5432/tests', {
        'min_size': 2, 'max_size': 2, 'convert_params': True}),
    'trio-mysql': ('trio-mysql://root@127.0.0.1:3306/tests', {}),

    # there is a separate test for triopg
    #  'triopg': ('triopg://test:test@localhost:5432/tests', {'min_size': 2, 'max_size': 2}),

    # Doesnt supports python 3.9
    #  'aioodbc': ('aioodbc://localhost', {
    #      'dsn': 'Driver=/usr/local/lib/libsqlite3odbc.dylib;Database=db.sqlite',
    #      'db_type': 'sqlite', 'maxsize': 2, 'minsize': 1,
    #  }),
}


@pytest.fixture(scope='session', params=[
    pytest.param(('asyncio', {'use_uvloop': False}), id='asyncio'),
    'trio'
])
def aiolib(request):
    """Support asyncio only. Disable uvloop on tests it brokes breakpoints."""
    return request.param


@pytest.fixture(scope='session', params=[name for name in BACKEND_PARAMS])
def backend(request):
    return request.param


@pytest.fixture
def db(backend, aiolib):
    from aio_databases import Database

    if aiolib[0] == 'trio' and backend not in {'trio-mysql', 'triopg'}:
        return pytest.skip()

    if aiolib[0] == 'asyncio' and backend not in {'aiomysql', 'aiopg', 'aiosqlite', 'asyncpg'}:
        return pytest.skip()

    url, params = BACKEND_PARAMS[backend]
    return Database(url, **params)


@pytest.fixture
async def pool(db):
    async with db:
        async with db.connection():
            yield db


@pytest.fixture(scope='session', autouse=True)
def setup_logging():
    logger = logging.getLogger('aio-databases')
    logger.setLevel(logging.DEBUG)


@pytest.fixture
def manager(db):
    return Manager(dialect=db.backend.db_type)


@pytest.fixture
def User():

    class User(Model):
        id = fields.Auto()
        name = fields.Varchar()
        fullname = fields.Varchar()

    return User


@pytest.fixture
def Comment(User):

    class Comment(Model):
        id = fields.Auto()
        body = fields.Varchar()

        user_id = fields.ForeignKey(User.id)

    return Comment
