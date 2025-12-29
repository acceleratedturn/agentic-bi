import os
import re
from datetime import datetime, timezone
import pandas as pd
import clickhouse_connect


def sanitize(name: str) -> str:
    name = name.strip().lower()
    name = re.sub(r"[^a-z0-9_]+", "_", name)
    name = re.sub(r"_+", "_", name).strip("_")
    if name[0].isdigit():
        name = f"c_{name}"
    return name


def pandas_to_ch(series: pd.Series) -> str:
    has_nulls = series.isna().any()
    if pd.api.types.is_integer_dtype(series):
        t = "Int64"
    elif pd.api.types.is_float_dtype(series):
        t = "Float64"
    elif pd.api.types.is_bool_dtype(series):
        t = "UInt8"
    elif pd.api.types.is_datetime64_any_dtype(series):
        t = "DateTime"
    else:
        t = "String"
    return f"Nullable({t})" if has_nulls else t

# Client Information stored in .env
def get_client():
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        database=os.getenv("CLICKHOUSE_DATABASE", "puredata"),
    )


# Automation for creating ClickHouse Table
def save_df_as_new_table(df: pd.DataFrame, source_file: str, table_name: str):
    client = get_client()
    db = os.getenv("CLICKHOUSE_DATABASE", "puredata")

    client.command(f"CREATE DATABASE IF NOT EXISTS {db}")

    clean_df = df.copy()
    clean_df.columns = [sanitize(c) for c in clean_df.columns]

    columns_sql = [
        "`ingested_at` DateTime",
        "`source_file` String",
    ]

    for col in clean_df.columns:
        ch_type = pandas_to_ch(clean_df[col])
        columns_sql.append(f"`{col}` {ch_type}")

    cols = ",\n  ".join(columns_sql)

    client.command(
        f"""
        CREATE TABLE IF NOT EXISTS {db}.`{table_name}` (
          {cols}
        )
        ENGINE = MergeTree
        ORDER BY ingested_at
        """
    )

    clean_df.insert(0, "ingested_at", datetime.now(timezone.utc).replace(tzinfo=None))
    clean_df.insert(1, "source_file", source_file)

    client.insert_df(f"{db}.{table_name}", clean_df)

    return {
        "database": db,
        "table": table_name,
        "rows": len(clean_df),
    }
