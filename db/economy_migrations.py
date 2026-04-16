from __future__ import annotations

from collections.abc import Sequence

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection


# Columns that can reasonably exceed 32-bit integer limits in production economies.
_BIGINT_TARGETS: tuple[tuple[str, str], ...] = (
    ("wallets", "silver"),
    ("wallets", "silver_earned"),
    ("wallets", "silver_spent"),
    ("slot_jackpots", "pool_silver"),
    ("bank_robbery_profiles", "lifetime_bankrobbery_earnings"),
)


def _target_filter_sql(targets: Sequence[tuple[str, str]]) -> str:
    return " OR ".join(f"(table_name = '{table}' AND column_name = '{column}')" for table, column in targets)


async def ensure_economy_bigint_columns(conn: AsyncConnection) -> list[str]:
    """Promote hot economy counters from INT to BIGINT when needed.

    Returns a list of altered `table.column` names.
    """
    if conn.dialect.name.lower() != "mysql":
        return []

    filter_sql = _target_filter_sql(_BIGINT_TARGETS)
    rows = (
        await conn.execute(
            text(
                f"""
                SELECT table_name, column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = DATABASE()
                  AND ({filter_sql})
                """
            )
        )
    ).mappings().all()

    found = {
        (str(row["table_name"]), str(row["column_name"])): str(row["data_type"]).lower()
        for row in rows
    }

    changed: list[str] = []
    for table_name, column_name in _BIGINT_TARGETS:
        data_type = found.get((table_name, column_name))
        if data_type is None or data_type == "bigint":
            continue
        await conn.exec_driver_sql(
            f"ALTER TABLE `{table_name}` MODIFY COLUMN `{column_name}` BIGINT NOT NULL DEFAULT 0"
        )
        changed.append(f"{table_name}.{column_name}")

    return changed
