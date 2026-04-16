from __future__ import annotations

import pytest

from db.economy_migrations import ensure_economy_bigint_columns


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return self

    def all(self):
        return self._rows


class _Dialect:
    def __init__(self, name: str):
        self.name = name


class _Conn:
    def __init__(self, *, dialect: str, rows):
        self.dialect = _Dialect(dialect)
        self._rows = rows
        self.alters: list[str] = []

    async def execute(self, _query):
        return _Result(self._rows)

    async def exec_driver_sql(self, sql: str):
        self.alters.append(sql)


@pytest.mark.asyncio
async def test_promotes_only_non_bigint_columns() -> None:
    conn = _Conn(
        dialect="mysql",
        rows=[
            {"table_name": "wallets", "column_name": "silver", "data_type": "int"},
            {"table_name": "wallets", "column_name": "silver_earned", "data_type": "bigint"},
            {"table_name": "wallets", "column_name": "silver_spent", "data_type": "int"},
        ],
    )

    changed = await ensure_economy_bigint_columns(conn)

    assert changed == ["wallets.silver", "wallets.silver_spent"]
    assert len(conn.alters) == 2
    assert "ALTER TABLE `wallets` MODIFY COLUMN `silver` BIGINT" in conn.alters[0]


@pytest.mark.asyncio
async def test_skips_non_mysql_connections() -> None:
    conn = _Conn(dialect="sqlite", rows=[])

    changed = await ensure_economy_bigint_columns(conn)

    assert changed == []
    assert conn.alters == []
