### My ingestion_date Jokes > 1900-01-01...

# Requirements:
# pip install pandas sqlalchemy psycopg2-binary

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime, timezone
from IPython.display import display  # useful when running in Jupyter

# -------------------------
# Postgres connection config
# -------------------------
PG_HOST = "postgres_dbt"   # or "localhost" if your container exposes the port
PG_PORT = 5432
PG_USER = "dbt_user"
PG_PASS = "dbt_password"
PG_DB   = "dbt_database"
SCHEMA  = "loadsmart"

engine = create_engine(
    f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}",
    client_encoding="utf8"
)

# -------------------------
# 0) Quick connection test
# -------------------------
try:
    with engine.connect() as conn:
        v = conn.execute(text("SELECT 1")).scalar()
    print("Connected OK (SELECT 1 returned ->", v, ")")
except Exception as e:
    raise RuntimeError(
        "Failed to connect to PostgreSQL. Check host/port/credentials and whether the container is running."
    ) from e

# -------------------------
# 1) List tables in schema
# -------------------------
query_list_tables = """
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema = :schema
  AND table_type = 'BASE TABLE'
ORDER BY table_name;
"""
df_tables = pd.read_sql_query(text(query_list_tables), engine, params={"schema": SCHEMA})
if df_tables.empty:
    print(f"No tables found in schema '{SCHEMA}'.")
else:
    print("Tables found in schema:")
    display(df_tables)


# -------------------------
# Helper: check column exists
# -------------------------
def table_has_column(engine, schema, table_name, column_name="ingestion_date"):
    """
    Returns True if the given table contains the specified column.
    Returns False if the table or column does not exist or on error.
    """
    q = text("""
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = :schema
      AND table_name = :table
      AND column_name = :column
    LIMIT 1;
    """)
    try:
        with engine.connect() as conn:
            row = conn.execute(q, {"schema": schema, "table": table_name, "column": column_name}).first()
            return row is not None
    except Exception as e:
        print("Warning: error checking column existence:", e)
        return False


# -------------------------
# Function: update ingestion_date for all rows
# -------------------------
def update_ingestion_date_all(engine, schema, table_name, new_timestamp):
    """
    Updates ingestion_date for ALL rows in the given table to new_timestamp.

    Parameters:
        engine: SQLAlchemy engine
        schema (str): schema name (e.g. 'loadsmart')
        table_name (str): table name (no schema)
        new_timestamp (str | datetime): target timestamp. Examples:
            - "2025-01-01 10:00:00"
            - datetime.utcnow()
    """

    # Validate that the table has ingestion_date column
    if not table_has_column(engine, schema, table_name, "ingestion_date"):
        raise RuntimeError(f'Table "{schema}.{table_name}" does not contain an "ingestion_date" column.')

    # Normalize new_timestamp to a Python datetime (UTC-aware) when possible
    ts_param = new_timestamp
    if isinstance(new_timestamp, str):
        # try parsing common formats; if fails, leave as string (Postgres can parse many formats)
        try:
            ts_dt = datetime.fromisoformat(new_timestamp)
            if ts_dt.tzinfo is None:
                ts_dt = ts_dt.replace(tzinfo=timezone.utc)
            else:
                ts_dt = ts_dt.astimezone(timezone.utc)
            ts_param = ts_dt
        except Exception:
            # keep string; SQL will attempt to parse it
            ts_param = new_timestamp
    elif isinstance(new_timestamp, datetime):
        if new_timestamp.tzinfo is None:
            ts_param = new_timestamp.replace(tzinfo=timezone.utc)
        else:
            ts_param = new_timestamp.astimezone(timezone.utc)

    # Perform update in a transaction
    update_sql = text(f'UPDATE "{schema}"."{table_name}" SET ingestion_date = :ts')
    try:
        with engine.begin() as conn:
            result = conn.execute(update_sql, {"ts": ts_param})
            # result.rowcount is not always reliable with all backends, but we try to print it if available
            try:
                affected = result.rowcount
            except Exception:
                affected = None

        print(f'✔️ Updated ingestion_date for all rows in "{schema}"."{table_name}".')
        if affected is not None:
            print(f"Rows affected: {affected}")
    except SQLAlchemyError as e:
        raise RuntimeError(f"Failed to update ingestion_date on {schema}.{table_name}: {e}") from e


# -------------------------
# Example usage
# -------------------------

# Replace with the table you want to update
TABLE_TO_UPDATE = "2025_data_challenge_ae"

# Example 1: set ingestion_date to a specific string (Postgres-friendly format)
new_ts_string = "1900-12-01 12:00:00"
update_ingestion_date_all(engine, SCHEMA, TABLE_TO_UPDATE, new_ts_string)

# Example 2: set ingestion_date to current UTC time (uncomment to use)
# update_ingestion_date_all(engine, SCHEMA, TABLE_TO_UPDATE, datetime.utcnow())

# Quick preview after update (top 5 rows)
try:
    q_preview = text(f'SELECT * FROM "{SCHEMA}"."{TABLE_TO_UPDATE}" LIMIT 5;')
    df_preview = pd.read_sql_query(q_preview, engine)
    print(f"\nPreview of {SCHEMA}.{TABLE_TO_UPDATE} (top 5 rows):")
    display(df_preview)
except Exception as e:
    print("Could not preview table after update:", e)
