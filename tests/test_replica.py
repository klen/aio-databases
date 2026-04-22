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


async def test_replica_disconnet():
    db = Database("sqlite:///:memory:", replicas=["sqlite:///:memory:"])

    async with db.replica():
        conn = db.current_conn
        await db.disconnect()
        assert db.current_conn is None
        assert not conn.is_ready


async def test_disconnect_inside_replica_context_exit_is_safe():
    db = Database("sqlite:///:memory:", replicas=["sqlite:///:memory:"])

    await db.connect()

    async with db.replica():
        await db.disconnect()


async def test_replica_uses_own_connection(tmp_path):
    """Verify that replica reads from its own database, not the primary."""
    primary_path = tmp_path / "primary.db"
    replica_path = tmp_path / "replica.db"

    # Seed primary and replica with the same schema but different data
    db_primary = Database(f"sqlite:///{primary_path}")
    async with db_primary:
        await db_primary.execute("CREATE TABLE items (name TEXT)")
        await db_primary.execute("INSERT INTO items VALUES ('primary-item')")

    db_replica = Database(f"sqlite:///{replica_path}")
    async with db_replica:
        await db_replica.execute("CREATE TABLE items (name TEXT)")
        await db_replica.execute("INSERT INTO items VALUES ('replica-item')")
        await db_replica.execute("INSERT INTO items VALUES ('replica-item-2')")

    # Now use the combined Database with replica configured
    db = Database(f"sqlite:///{primary_path}", replicas=[f"sqlite:///{replica_path}"])

    async with db:
        # Primary sees its own data
        names = await db.fetchall("SELECT name FROM items")
        assert names == [("primary-item",)]

        async with db.replica():
            # Replica sees its own data (two rows)
            names = await db.fetchall("SELECT name FROM items")
            assert {n[0] for n in names} == {"replica-item", "replica-item-2"}

            count = await db.fetchval("SELECT COUNT(*) FROM items")
            assert count == 2

        # Back to primary: still one row
        count = await db.fetchval("SELECT COUNT(*) FROM items")
        assert count == 1
