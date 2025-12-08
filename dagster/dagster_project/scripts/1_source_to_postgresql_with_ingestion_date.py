## source to postgresql + ingestion_date + lane expansion

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
# 0. EXPAND LANE COLUMN INTO 4 FIELDS
# =========================================

def expand_lane_columns(df):
    """
    Se existir a coluna 'lane', cria as colunas:
    - pickup_city
    - pickup_state
    - delivery_city
    - delivery_state

    A função é robusta: qualquer valor inválido retorna None nos campos.
    """

    if "lane" not in df.columns:
        return df  # nada a fazer

    def split_lane(value):
        try:
            if pd.isna(value):
                return pd.Series([None, None, None, None])

            value = str(value).strip()

            # separar pickup e delivery
            pickup, delivery = value.split("->")
            pickup = pickup.strip()
            delivery = delivery.strip()

            # separar city e state
            pickup_city, pickup_state = [x.strip() for x in pickup.split(",")]
            delivery_city, delivery_state = [x.strip() for x in delivery.split(",")]

            return pd.Series([pickup_city, pickup_state, delivery_city, delivery_state])

        except Exception:
            # caso algo dê errado na linha
            return pd.Series([None, None, None, None])

    df[["pickup_city", "pickup_state", "delivery_city", "delivery_state"]] = (
        df["lane"].apply(split_lane)
    )

    return df


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
            row = conn.execute(q, {
                "schema": schema,
                "table": table_name,
                "column": column_name
            }).first()
            return row is not None
    except Exception:
        return False


def get_max_ingestion(engine, schema, table_name):
    if not table_has_column(engine, schema, table_name, "ingestion_date"):
        return None

    q = text(f'SELECT MAX(ingestion_date) FROM "{schema}"."{table_name}";')
    try:
        with engine.connect() as conn:
            mx = conn.execute(q).scalar()
    except Exception:
        return None

    if mx is None:
        return None

    try:
        if mx.tzinfo is None:
            mx = mx.replace(tzinfo=timezone.utc)
        else:
            mx = mx.astimezone(timezone.utc)
    except Exception:
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
# 6. MAIN PIPELINE
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
        except IOError:
            continue

        file_mtime_utc = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
        print(f"SFTP mtime (UTC): {file_mtime_utc}")

        # Table existence
        exists = table_exists(engine, SCHEMA, table_name)

        if not exists:
            should_process = True
        else:
            max_ing = get_max_ingestion(engine, SCHEMA, table_name)
            if max_ing is None or file_mtime_utc > max_ing:
                should_process = True
            else:
                should_process = False

        if not should_process:
            continue

        # Download file
        local_tmp = f"/tmp/{csv_file}"
        sftp.get(remote_path, local_tmp)

        # Create DataFrame
        df = pd.read_csv(local_tmp)

        # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
        # EXPAND 'lane' INTO 4 NEW COLUMNS
        # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
        df = expand_lane_columns(df)

        # Recreate table + insert data
        drop_and_create_table(df, engine, SCHEMA, table_name)
        insert_into_postgres(df, engine, SCHEMA, table_name)

        print(f"🏁 Finished processing {SCHEMA}.{table_name}")

        try:
            os.remove(local_tmp)
        except Exception:
            pass

    sftp.close()
    print("\nAll files evaluated. Pipeline complete.")


# Run pipeline
if __name__ == "__main__":
    run_all()
