# src/puredata/database/clickhouse_client.py

import os
import re
import clickhouse_connect
import pandas as pd


# Block dangerous SQL keywords
BLOCKED_KEYWORDS = [
    "insert", "update", "delete", "drop", "truncate",
    "alter", "create", "attach", "detach", "grant",
    "revoke", "optimize"
]


def get_clickhouse_client():
    """Create and return a ClickHouse client (native protocol)."""
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        database=os.getenv("CLICKHOUSE_DATABASE", "puredata"),
    )


def validate_readonly_query(sql: str) -> str:
    """Ensure the query is SELECT-only."""
    sql = sql.strip().rstrip(";")
    sql_lower = sql.lower()

    if not (sql_lower.startswith("select") or sql_lower.startswith("with")):
        raise ValueError("Only SELECT queries are allowed.")

    for keyword in BLOCKED_KEYWORDS:
        if re.search(rf"\b{keyword}\b", sql_lower):
            raise ValueError(f"Blocked keyword detected: {keyword}")

    return sql


def enforce_limit(sql: str, max_rows: int) -> str:
    """Force LIMIT if missing to prevent large pulls."""
    if re.search(r"\blimit\b", sql, re.IGNORECASE):
        return sql
    return f"{sql} LIMIT {max_rows}"


def run_read_query(sql: str, max_rows: int = 5000) -> pd.DataFrame:
    """
    Execute a safe, read-only query and return a DataFrame.
    """
    sql = validate_readonly_query(sql)
    sql = enforce_limit(sql, max_rows)

    client = get_clickhouse_client()
    return client.query_df(sql)
