import pandas as pd
import os
from typing import Optional, Any
from crewai.tools import tool

from puredata.database.clickhouse_read import run_read_query

@tool("ClickHouse_Read")
def clickhouse_read(sql: str, max_rows: int = 5000, preview_rows: int = 10) -> str:
    """
    Read-only ClickHouse query tool for Agent 2.

    - Allows SELECT/WITH...SELECT only (enforced in run_read_query)
    - Enforces LIMIT if missing (enforced in run_read_query)
    - Returns a small preview to save tokens
    """
    try:
        df = run_read_query(sql=sql, max_rows=max_rows)

        n = min(preview_rows, len(df))
        preview = df.head(n).to_string(index=False)

        return (
            f"ClickHouse_Read success\n"
            f"Rows returned: {len(df)}\n"
            f"Columns: {', '.join(df.columns)}\n\n"
            f"Preview (first {n} rows):\n{preview}"
        )

    except Exception as e:
        return f"ClickHouse_Read failed: {e}"