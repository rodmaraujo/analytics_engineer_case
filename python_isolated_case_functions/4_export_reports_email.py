import os
import pandas as pd
import paramiko
from sqlalchemy import create_engine, text
from datetime import datetime

# =========================================
# CONFIGURATION
# =========================================

PG_HOST = "postgres_dbt"
PG_PORT = 5432
PG_USER = "dbt_user"
PG_PASS = "dbt_password"
PG_DB   = "dbt_database"

SFTP_HOST = "edge13.ftpgrid.com"
SFTP_PORT = 22
SFTP_USER = "M9T.qt088q5s"
SFTP_PASS = "RJ|oGkfK43UJ$U27loOZ]5Fx5$R6[|>C"

REMOTE_DIR = "/reports"

TABLES = {
    "loadsmart_id_lastmonth_report": "loadsmart_reports.loadsmart_id_lastmonth_report",
    "anl_carrier": "loadsmart_anl.anl_carrier",
    "anl_pickup_location": "loadsmart_anl.anl_pickup_location",
    "anl_delivery_location": "loadsmart_anl.anl_delivery_location"
}

# =========================================
# SFTP FUNCTIONS
# =========================================

def sftp_connect():
    """Establish and return an SFTP connection."""
    transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
    transport.connect(username=SFTP_USER, password=SFTP_PASS)
    return paramiko.SFTPClient.from_transport(transport)


def ensure_sftp_directory(sftp, directory):
    """Create the directory on SFTP if it does not exist."""
    try:
        sftp.listdir(directory)
    except IOError:
        print(f"📁 Creating missing directory: {directory}")
        sftp.mkdir(directory)


def clear_sftp_folder(sftp, directory):
    """Delete all existing files inside the directory."""
    try:
        files = sftp.listdir(directory)
        for f in files:
            full_path = f"{directory}/{f}"
            print(f"🗑 Removing old file: {full_path}")
            sftp.remove(full_path)
    except IOError as e:
        print("No files to delete or cannot access directory:", e)


# =========================================
# POSTGRES FUNCTIONS
# =========================================

def get_dataframe(engine, table):
    """Run SELECT * and return a DataFrame."""
    query = f"SELECT * FROM {table}"
    return pd.read_sql(query, engine)


# =========================================
# MAIN PROCESS
# =========================================

def export_reports():
    """Exports PostgreSQL tables as CSV files and uploads them to SFTP."""

    # Connect PostgreSQL
    print("\n🔌 Connecting to PostgreSQL...")
    engine = create_engine(
        f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    )
    print("Connected to PostgreSQL.")

    # Connect SFTP
    print("\n🔌 Connecting to SFTP...")
    sftp = sftp_connect()
    print("Connected to SFTP.")

    # Ensure /reports folder exists
    ensure_sftp_directory(sftp, REMOTE_DIR)

    # Clear previous files
    clear_sftp_folder(sftp, REMOTE_DIR)

    # Today's date for filenames
    today_str = datetime.now().strftime("%Y%m%d")

    # Export all tables
    for short_name, full_table in TABLES.items():
        print(f"\n📄 Exporting table: {full_table}")

        df = get_dataframe(engine, full_table)

        filename = f"{short_name}_{today_str}.csv"
        local_tmp = f"/tmp/{filename}"
        remote_path = f"{REMOTE_DIR}/{filename}"

        # Save locally
        df.to_csv(local_tmp, index=False)

        print(f"⬆️ Uploading {filename} to SFTP...")
        sftp.put(local_tmp, remote_path)

        print(f"✔️ Uploaded: {remote_path}")

        # Remove temp file
        os.remove(local_tmp)

    # Close SFTP
    sftp.close()

    print("\n🏁 EXPORT COMPLETE — All reports uploaded successfully.\n")


# =========================================
# RUN
# =========================================

if __name__ == "__main__":
    export_reports()
