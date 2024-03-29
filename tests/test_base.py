import pytest


def test_backends(arm: bool):
    from aio_databases.backends import BACKENDS

    assert BACKENDS
    db_types = {b.db_type for b in BACKENDS}
    assert "mysql" in db_types
    assert "sqlite" in db_types
    assert "postgresql" in db_types
    assert arm or "odbc" in db_types


def test_database(arm: bool):
    from aio_databases import Database

    with pytest.raises(ValueError, match="Unknown backend"):
        Database("unknown://db.sqlite")

    assert Database("dummy://unknown")
    assert Database("mysql://db.sqlite")
    assert Database("sqlite://db.sqlite")
    assert Database("postgresql://db.sqlite")
    assert Database("postgres://db.sqlite")

    assert Database("aiomysql://db.sqlite")
    assert Database("aiosqlite://db.sqlite")
    assert Database("asyncpg://db.sqlite")

    assert Database("trio-mysql://localhost")
    # assert Database("triopg://localhost")  # noqa: ERA

    if not arm:
        assert Database("aioodbc://localhost", dsn="Driver=SQLite;Database=db.sqlite")


def test_record():
    from aio_databases.record import Record

    rec = Record((1, "test"), [["id"], ["name"]])
    assert rec
    assert len(rec) == 2
    assert rec[0] == rec["id"] == 1
    assert rec[1] == rec["name"] == "test"
    assert tuple(rec) == (1, "test")
    assert "name" in rec
    assert "value" not in rec
    assert list(rec.values()) == [1, "test"]
    assert list(rec.keys()) == ["id", "name"]
    assert tuple(rec.items()) == (("id", 1), ("name", "test"))
    assert rec == (1, "test")

    assert rec == Record.from_dict({"id": 1, "name": "test"})
    assert str(rec) == "id=1 name='test'"
    assert repr(rec) == "<Record id=1 name='test'>"

    rec = Record((1, "test", 2, "test2"), [["id"], ["name"], ["id"], ["name"]])
    assert list(rec) == [1, "test", 2, "test2"]
    assert list(rec.values()) == [1, "test", 2, "test2"]
    assert list(rec.keys()) == ["id", "name", "id", "name"]
    assert str(rec) == "id=1 name='test' id=2 name='test2'"


def test_params():
    from aio_databases import Database

    db = Database("asyncpg://localhost", convert_params=True)
    assert db.backend.__convert_sql__('select "%s", %s') == 'select "$1", $2'
    assert db.backend.__convert_sql__('select "%%s"') == 'select "%%s"'

    db = Database("aiosqlite://localhost", convert_params=True)
    assert db.backend.__convert_sql__('select "%s", %s') == 'select "?", ?'
    assert db.backend.__convert_sql__('select "%%s"') == 'select "%%s"'
