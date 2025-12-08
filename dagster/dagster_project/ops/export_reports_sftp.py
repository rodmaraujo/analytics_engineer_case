import os
import pandas as pd
import paramiko
from sqlalchemy import create_engine
from datetime import datetime
from dagster import op, get_dagster_logger

PG_HOST = "postgres"
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


def sftp_connect():
    transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
    transport.connect(username=SFTP_USER, password=SFTP_PASS)
    return paramiko.SFTPClient.from_transport(transport)


def ensure_sftp_directory(sftp, directory):
    try:
        sftp.listdir(directory)
    except IOError:
        sftp.mkdir(directory)


def clear_sftp_folder(sftp, directory):
    try:
        for f in sftp.listdir(directory):
            sftp.remove(f"{directory}/{f}")
    except IOError:
        pass


def get_dataframe(engine, table):
    return pd.read_sql(f"SELECT * FROM {table}", engine)


@op
def export_reports_sftp(context, upstream):
    logger = get_dagster_logger()

    logger.info(f"Starting report export process. Upstream message: {upstream}")

    engine = create_engine(
        f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    )

    logger.info("Connecting to SFTP server...")
    sftp = sftp_connect()
    logger.info("Connected to SFTP.")

    ensure_sftp_directory(sftp, REMOTE_DIR)
    clear_sftp_folder(sftp, REMOTE_DIR)

    today_str = datetime.now().strftime("%Y%m%d")

    for short_name, full_table in TABLES.items():
        logger.info(f"Exporting table: {full_table}")

        df = get_dataframe(engine, full_table)

        filename = f"{short_name}_{today_str}.csv"
        local_tmp = f"/tmp/{filename}"
        remote_path = f"{REMOTE_DIR}/{filename}"

        df.to_csv(local_tmp, index=False)

        logger.info(f"Uploading file to SFTP: {filename}")
        sftp.put(local_tmp, remote_path)

        os.remove(local_tmp)

    sftp.close()

    logger.info("All reports exported successfully.")
    return "EXPORT_DONE"
