## source to postgresql + ingestion_date 

# Requirements:
# pip install pandas paramiko psycopg2-binary sqlalchemy

import os
import pandas as pd
import paramiko
from io import StringIO
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime, timezone

# =========================================
# 1. SFTP CONNECTION
# =========================================

def sftp_connect(host, port, username, password):
    transport = paramiko.Transport((host, port))
    transport.connect(username=username, password=password)
    return paramiko.SFTPClient.from_transport(transport)


# =========================================
# 2. SCHEMA CHECK
# =========================================

def ensure_schema(engine, schema):
    with engine.connect() as con:
        con.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
        con.commit()


# =========================================
# 3. DROP + CREATE TABLE WITH ingestion_date
# =========================================

def drop_and_create_table(df, engine, schema, table_name):
    print(f"\n🛠 RECREATING TABLE {schema}.{table_name}")
    ensure_schema(engine, schema)

    # Drop table if it exists
    with engine.begin() as con:
        con.execute(text(f'DROP TABLE IF EXISTS "{schema}"."{table_name}" CASCADE'))

    # Build DDL from DataFrame dtypes
    col_defs = []
    for col, dtype in df.dtypes.items():
        if "int" in str(dtype):
            pg_type = "BIGINT"
        elif "float" in str(dtype):
            pg_type = "DOUBLE PRECISION"
        elif "bool" in str(dtype):
            pg_type = "BOOLEAN"
        else:
            pg_type = "TEXT"
        col_defs.append(f'"{col}" {pg_type}')

    # Guaranteed ingestion_date column
    col_defs.append('ingestion_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP')

    ddl = f"""
        CREATE TABLE "{schema}"."{table_name}" (
            {",".join(col_defs)}
        );
    """

    with engine.begin() as con:
        con.execute(text(ddl))

    print("✔️ Table created WITH ingestion_date")


# =========================================
# 4. INSERT WITH COPY — CSV ONLY, NO ingestion_date
# =========================================

def insert_into_postgres(df, engine, schema, table_name):
    print(f"📥 Inserting {len(df)} rows into {schema}.{table_name}...")

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, header=True)
    csv_buffer.seek(0)

    # Only CSV columns — DO NOT INCLUDE ingestion_date
    csv_columns = [f'"{c}"' for c in df.columns]

    conn = engine.raw_connection()
    try:
        cursor = conn.cursor()
        cursor.copy_expert(
            f"""
            COPY "{schema}"."{table_name}" ({",".join(csv_columns)})
            FROM STDIN WITH CSV HEADER;
            """,
            csv_buffer
        )
        conn.commit()
    finally:
        conn.close()

    print("✔️ Data inserted successfully.")


# =========================================
# 5. HELPERS — VALIDATION FUNCTIONS
# =========================================

def table_has_column(engine, schema, table_name, column_name="ingestion_date"):
    """
    Returns True if the given table contains the specified column.
    Returns False if table or column does not exist.
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
            row = conn.execute(q, {"schema": schema,
                                   "table": table_name,
                                   "column": column_name}).first()
            return row is not None
    except Exception as e:
        print("Warning: error checking column existence:", e)
        return False


def get_max_ingestion(engine, schema, table_name):
    """
    Returns MAX(ingestion_date) as a timezone-aware UTC datetime.
    Returns None if table or column does not exist, or no rows are present.
    """

    # If table has no ingestion_date column, return None
    if not table_has_column(engine, schema, table_name, "ingestion_date"):
        return None

    q = text(f'SELECT MAX(ingestion_date) FROM "{schema}"."{table_name}";')

    try:
        with engine.connect() as conn:
            mx = conn.execute(q).scalar()
    except SQLAlchemyError as e:
        print("Error executing MAX(ingestion_date):", e)
        return None
    except Exception as e:
        print("Unexpected error executing query:", e)
        return None

    if mx is None:
        return None

    # Normalize to timezone-aware UTC
    try:
        if getattr(mx, "tzinfo", None) is None:
            mx = mx.replace(tzinfo=timezone.utc)
        else:
            mx = mx.astimezone(timezone.utc)
    except Exception as e:
        print("Warning normalizing timestamp:", e)
        return None

    return mx


def table_exists(engine, schema, table_name):
    q = text("""
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = :schema
        AND table_name = :table
    );
    """)
    with engine.connect() as conn:
        return conn.execute(q, {"schema": schema, "table": table_name}).scalar()


# =========================================
# 6. MAIN PIPELINE (WITH SFTP mtime vs max ingestion_date)
# =========================================

def run_all():

    # SFTP credentials
    HOST = "edge13.ftpgrid.com"
    PORT = 22
    USER = "M9T.qt088q5s"
    PASS = "RJ|oGkfK43UJ$U27loOZ]5Fx5$R6[|>C"
    DIRECTORY = "/documents"

    # Postgres credentials
    PG_HOST = "postgres_dbt"
    PG_PORT = 5432
    PG_USER = "dbt_user"
    PG_PASS = "dbt_password"
    PG_DB = "dbt_database"
    SCHEMA = "loadsmart"

    engine = create_engine(
        f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    )

    print("\n=== STEP 1: Connecting to SFTP ===")
    sftp = sftp_connect(HOST, PORT, USER, PASS)
    print("Connected to SFTP.")

    try:
        files = sftp.listdir(DIRECTORY)
    except IOError as e:
        print("Failed to list SFTP directory:", e)
        return

    csv_files = [f for f in files if f.lower().endswith(".csv")]
    print("\nCSV files found:", csv_files)

    if not csv_files:
        print("No CSV files found.")
        return

    for csv_file in csv_files:
        table_name = csv_file.replace(".csv", "").lower()
        remote_path = f"{DIRECTORY}/{csv_file}"

        print(f"\n➡️ Evaluating {csv_file} -> table: {SCHEMA}.{table_name}")

        # Get SFTP modification time
        try:
            stat = sftp.stat(remote_path)
        except IOError as e:
            print("Failed to stat file:", e)
            continue

        file_mtime_utc = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
        print(f"SFTP mtime (UTC): {file_mtime_utc}")

        # Table existence
        exists = table_exists(engine, SCHEMA, table_name)

        if not exists:
            print("Table does not exist → will process file.")
            should_process = True
        else:
            max_ing = get_max_ingestion(engine, SCHEMA, table_name)

            if max_ing is None:
                print("Table exists but has no ingestion_date → will process file.")
                should_process = True
            else:
                print(f"Table max ingestion_date (UTC): {max_ing}")

                if file_mtime_utc > max_ing:
                    print("File is newer than table → will process.")
                    should_process = True
                else:
                    print("File is NOT newer than table → skipping.")
                    should_process = False

        if not should_process:
            continue

        # Download file
        local_tmp = f"/tmp/{csv_file}"
        print(f"Downloading {remote_path} to {local_tmp} ...")
        sftp.get(remote_path, local_tmp)
        print("Download complete.")

        # Create DataFrame
        df = pd.read_csv(local_tmp)

        # Recreate table + insert data
        drop_and_create_table(df, engine, SCHEMA, table_name)
        insert_into_postgres(df, engine, SCHEMA, table_name)

        print(f"🏁 Finished processing {SCHEMA}.{table_name}")

        # Remove temporary file
        try:
            os.remove(local_tmp)
        except Exception:
            pass

    sftp.close()
    print("\nAll files evaluated. Pipeline complete.")


# Run pipeline
if __name__ == "__main__":
    run_all()
