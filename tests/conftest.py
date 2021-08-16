import pytest

from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, ForeignKey


@pytest.fixture(scope='session')
def aiolib():
    """Support asyncio only. Disable uvloop on tests it brokes breakpoints."""
    return ('asyncio', {'use_uvloop': False})


@pytest.fixture
def engine(db):
    return create_engine(db.url, echo=True)


@pytest.fixture
def metadata(engine):
    meta = MetaData()
    return meta


@pytest.fixture
def users(metadata):
    return Table(
        'users', metadata,
        Column('id', Integer, primary_key=True),
        Column('name', String(length=256)),
        Column('fullname', String(length=256)),
    )


@pytest.fixture
def addresses(metadata):
    return Table(
        'addresses', metadata,
        Column('id', Integer, primary_key=True),
        Column('user_id', None, ForeignKey('users.id')),
        Column('email_address', String(length=256), nullable=False)
    )
