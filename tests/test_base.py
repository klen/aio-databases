import pytest


def test_backends():
    from aio_databases.backends import BACKENDS

    assert BACKENDS
    assert BACKENDS['sqlite']
    assert BACKENDS['postgresql']
    assert BACKENDS['mysql']


def test_database():
    from aio_databases import Database

    with pytest.raises(ValueError):
        db = Database('unknown://db.sqlite')

    db = Database('sqlite://db.sqlite')
    assert db


def test_record():
    from aio_databases.record import Record

    rec = Record((1, 'test'), [['id'], ['name']])
    assert rec
    assert len(rec) == 2
    assert rec[0] == rec['id'] == 1
    assert rec[1] == rec['name'] == 'test'
    assert tuple(rec) == (1, 'test')
    assert 'name' in rec
    assert 'value' not in rec
    assert rec.values() == (1, 'test')
    assert rec.keys() == ('id', 'name')
    assert rec.items() == (('id', 1), ('name', 'test'))
    assert rec == (1, 'test')

    assert rec == Record.from_dict({'id': 1, 'name': 'test'})
    assert str(rec) == "id=1 name='test'"
    assert repr(rec) == "<Record id=1 name='test'>"
