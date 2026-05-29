# AGENTS.md — aio-databases

Async DB library for SQLite, PostgreSQL, MySQL. Supports asyncio and (limited) trio.
Drivers: aiosqlite, asyncpg, aiopg, aiomysql, trio_mysql, aioodbc.

## Key paths
- `aio_databases/database.py` — Core `Database` class
- `aio_databases/backends/` — Driver backends (`common.py` is the base for all)
- `aio_databases/record.py` — Row record abstraction
- `aio_databases/types.py` — Type definitions
- `aio_databases/url.py` — URL parsing and redaction
- `tests/conftest.py` — Shared fixtures, backend parametrization
- `pyproject.toml` — Project config (ruff 100 cols, pyrefly, pytest)
- `uv.lock` — Locked deps (auto-generated, do not edit)

## Tools
- **Package manager**: `uv` only. No pip/poetry.
- **Lint/format**: `uv run ruff check`, `uv run ruff format`
- **Type check**: `uv run pyrefly check`
- **Tests**: `uv run pytest ...` (SQLite-only tests need no Docker)
- **Commits**: conventional; types in `.git-commits.yaml`

## Safe validation
```bash
uv sync
uv run ruff check && uv run pyrefly check
uv run pytest tests/test_base.py tests/test_sqlite.py tests/test_queries.py tests/test_connections.py tests/test_transactions.py
make test  # full suite; needs Docker postgres + mysql
```

## Editing rules
- Python 3.10+, `from __future__ import annotations`, `|` unions
- 100 char line limit
- Backends: `common.py` changes affect `_asyncpg.py`, `_aiomysql.py`, `_aiopg.py`, `_aiosqlite.py`, `_aioodbc.py`, `_trio_mysql.py`
- Tests: prefer `db` fixture tests over backend internals
- Do not commit `uv.lock` unless deps intentionally changed

## High-risk: avoid unless asked
- `make release` / `make patch` / `make minor` — bumps version, tags, merges develop→main, pushes
- `uv publish` — PyPI publish
- `docker start postgres mysql` — fails without containers
- Manual edits to `uv.lock` or version in `pyproject.toml`

## Avoid touching
`uv.lock`, `Changelog`, `db.sqlite`, `*.egg-info/`, `.git-commits.yaml`, tool caches (`.tox/`, `.pytest_cache/`, `.mypy_cache/`, `.ruff_cache/`).

## Notes
- Default branch: `develop`. `main` is stable. PRs to `develop`.
- Only `trio-mysql` supports trio; others are asyncio-only.
- Drivers are optional extras; core has no heavy runtime deps.
