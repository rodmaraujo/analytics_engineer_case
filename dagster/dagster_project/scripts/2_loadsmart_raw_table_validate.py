### raw_data validade


# Requirements: pip install sqlalchemy psycopg2-binary pandas
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError

PG_HOST = "postgres_dbt"   # or "localhost" if the container exposes the port
PG_PORT = 5432
PG_USER = "dbt_user"
PG_PASS = "dbt_password"
PG_DB   = "dbt_database"
SCHEMA  = "loadsmart"

engine = create_engine(
    f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}",
    client_encoding="utf8"
)

# 0) Connection test
try:
    with engine.connect() as conn:
        v = conn.execute(text("SELECT 1")).scalar()
    print("Connected successfully (SELECT 1 returned ->", v, ")")
except Exception as e:
    raise RuntimeError(
        "Failed to connect to PostgreSQL. Check host/port/credentials and whether the container is running."
    ) from e

# 1) List tables in the schema
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
    display(df_tables)

# 2) Set TABLE_NAME if you want to inspect a specific table
TABLE_NAME = None   

tables_to_inspect = []

if TABLE_NAME:
    tables_to_inspect = [TABLE_NAME]
else:
    # automatically inspect up to 5 tables (you can adjust this limit)
    tables_to_inspect = df_tables["table_name"].tolist()[:5]

# 3) Read TOP 10 rows from each table selected
for tbl in tables_to_inspect:
    print(f"\n--- Top 10 rows from: {SCHEMA}.{tbl} ---")
    q = f'SELECT * FROM "{SCHEMA}"."{tbl}" LIMIT 10;'
    try:
        df_top = pd.read_sql_query(text(q), engine)
        if df_top.empty:
            print("(table exists but contains no rows)")
        else:
            display(df_top)
    except ProgrammingError as pe:
        print("Error reading table:", pe)
    except Exception as e:
        print("Unexpected error reading table:", e)
