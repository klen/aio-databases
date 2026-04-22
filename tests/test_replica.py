import pytest

from aio_databases import Database, ReadOnlyError


@pytest.fixture
def backend():
    return "aiosqlite"


async def test_replica_no_replicas_configured():
    db = Database("sqlite:///:memory:")
    with pytest.raises(RuntimeError, match="No replicas configured"):
        db.replica()


async def test_replica_fetch_queries_work():
    db = Database("sqlite:///:memory:", replicas=["sqlite:///:memory:"])

    async with db, db.replica():
        result = await db.fetchval("SELECT 42")
        assert result == 42


async def test_replica_execute_raises():
    db = Database("sqlite:///:memory:", replicas=["sqlite:///:memory:"])

    async with db, db.replica():
        with pytest.raises(ReadOnlyError, match="not allowed on read-only"):
            await db.execute("CREATE TABLE t (x INT)")


async def test_replica_executemany_raises():
    db = Database("sqlite:///:memory:", replicas=["sqlite:///:memory:"])

    async with db, db.replica():
        with pytest.raises(ReadOnlyError, match="not allowed on read-only"):
            await db.executemany("INSERT INTO t VALUES (?)", (1,), (2,))


async def test_replica_nesting_with_primary_connection(tmp_path):
    db_path = tmp_path / "primary.db"
    db = Database(f"sqlite:///{db_path}", replicas=[f"sqlite:///{tmp_path / 'replica.db'}"])

    async with db:
        # Primary connection for setup
        await db.execute("CREATE TABLE t (x INT)")

        async with db.replica():
            # Read works on replica (separate db, just query a literal)
            assert await db.fetchval("SELECT 42") == 42

            # Write fails on replica
            with pytest.raises(ReadOnlyError):
                await db.execute("CREATE TABLE r (x INT)")

            # Nested primary connection allows writes
            async with db.connection():
                await db.execute("INSERT INTO t VALUES (1)")

            # Back to replica: write fails again
            with pytest.raises(ReadOnlyError):
                await db.execute("INSERT INTO t VALUES (2)")

        # Outside replica: writes work normally
        await db.execute("INSERT INTO t VALUES (3)")
        assert await db.fetchval("SELECT COUNT(*) FROM t") == 2


async def test_replica_transaction_not_allowed():
    db = Database("sqlite:///:memory:", replicas=["sqlite:///:memory:"])

    async with db, db.replica():
        with pytest.raises(ReadOnlyError, match="not allowed on read-only"):
            async with db.transaction():
                result = await db.fetchval("SELECT 42")
                assert result == 42
