# AIO-Databases

The package gives you asycio support for a range of databases (SQLite,
PostgreSQL, MySQL).

[![Tests Status](https://github.com/klen/aio-databases/workflows/tests/badge.svg)](https://github.com/klen/aio-databases/actions)
[![PYPI Version](https://img.shields.io/pypi/v/aio-databases)](https://pypi.org/project/aio-databases/)
[![Python Versions](https://img.shields.io/pypi/pyversions/aio-databases)](https://pypi.org/project/aio-databases/)


## Requirements

* python >= 3.7

## Installation

**aio-databases** should be installed using pip:

```shell
$ pip install aio-databases
```

You can install the required database drivers with:

```shell
$ pip install aio-databases[sqlite]
$ pip install aio-databases[postgresql]
$ pip install aio-databases[mysql]
```


## Usage

```python
    from aio_databases import Database

    db = Database('sqlite:///:memory:')

    await db.execute('select $1', '1')
    await db.executemany('select $1', '1', '2', '3')

    res = await db.fetchall('select (2 * $1) res', 2)
    assert res == [(4,)]

    res = await db.fetchone('select (2 * $1) res', 2)
    assert res == (4,)
    assert isinstance(res, db.backend.record_cls)

    res = await db.fetchval('select 2 * $1', 2)
    assert res == 4

```

## Bug tracker

If you have any suggestions, bug reports or annoyances please report them to
the issue tracker at https://github.com/klen/aio-databases/issues


## Contributing

Development of the project happens at: https://github.com/klen/aio-databases


## License

Licensed under a [MIT License](http://opensource.org/licenses/MIT)
