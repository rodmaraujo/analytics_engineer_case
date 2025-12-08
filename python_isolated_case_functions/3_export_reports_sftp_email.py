

### IMPORTANT! THIS SCRIPT NEEDS THAT loadsmart_reports.loadsmart_id_lastmonth_report TABLE EXISTS

import os
import smtplib
import pandas as pd
import paramiko
from datetime import datetime
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email import encoders
from sqlalchemy import create_engine

# =========================================
# CONFIGURATION
# =========================================

# ---- PostgreSQL ----
PG_HOST = "postgres_dbt"
PG_PORT = 5432
PG_USER = "dbt_user"
PG_PASS = "dbt_password"
PG_DB   = "dbt_database"

# ---- SFTP ----
SFTP_HOST = "edge13.ftpgrid.com"
SFTP_PORT = 22
SFTP_USER = "M9T.qt088q5s"
SFTP_PASS = "RJ|oGkfK43UJ$U27loOZ]5Fx5$R6[|>C"
REMOTE_DIR = "/reports"

# ---- Mailtrap SMTP ----
SMTP_HOST = "sandbox.smtp.mailtrap.io"
SMTP_PORT = 587
SMTP_USER = "94114d3b144d33"
SMTP_PASS = "9cb8efdd8d2657"
FROM_EMAIL = "reports@test.com"

# ---- Final recipient ----
TO_EMAIL = "rodrigomartinsaraujo@gmail.com"

# ---- Tables to export ----
TABLES = {
    "loadsmart_id_lastmonth_report": "loadsmart_reports.loadsmart_id_lastmonth_report",
    "anl_carrier": "loadsmart_anl.anl_carrier",
    "anl_pickup_location": "loadsmart_anl.anl_pickup_location",
    "anl_delivery_location": "loadsmart_anl.anl_delivery_location"
}

# =========================================
# SFTP UTILS
# =========================================

def sftp_connect():
    transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
    transport.connect(username=SFTP_USER, password=SFTP_PASS)
    return paramiko.SFTPClient.from_transport(transport)

def ensure_sftp_directory(sftp, directory):
    try:
        sftp.listdir(directory)
    except IOError:
        print(f"Creating missing directory: {directory}")
        sftp.mkdir(directory)

def clear_sftp_folder(sftp, directory):
    try:
        for f in sftp.listdir(directory):
            full_path = f"{directory}/{f}"
            print(f"Removing old file: {full_path}")
            sftp.remove(full_path)
    except IOError:
        print("Directory empty or inaccessible.")

# =========================================
# POSTGRES FUNCTIONS
# =========================================

def get_dataframe(engine, table):
    return pd.read_sql(f"SELECT * FROM {table}", engine)

# =========================================
# EMAIL SENDER (SMTP Mailtrap → Gmail)
# =========================================

def send_email_reports(files_to_attach):
    print("\nSending email with attached reports to:", TO_EMAIL)

    msg = MIMEMultipart()
    msg["From"] = FROM_EMAIL
    msg["To"] = TO_EMAIL
    msg["Subject"] = "Automated Reports Delivery"

    msg.attach(MIMEText("Attached are the latest exported reports.", "plain"))

    for file_path in files_to_attach:
        filename = os.path.basename(file_path)
        print(f"Attaching: {filename}")

        with open(file_path, "rb") as f:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", f'attachment; filename="{filename}"')
            msg.attach(part)

    server = smtplib.SMTP(SMTP_HOST, SMTP_PORT)
    server.starttls()
    server.login(SMTP_USER, SMTP_PASS)
    server.sendmail(FROM_EMAIL, [TO_EMAIL], msg.as_string())
    server.quit()

    print("Email successfully sent!")

# =========================================
# MAIN FUNCTION
# =========================================

def export_reports():
    print("\nConnecting to PostgreSQL...")
    engine = create_engine(
        f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    )
    print("Connected to PostgreSQL.")

    print("\nConnecting to SFTP...")
    sftp = sftp_connect()
    print("Connected to SFTP.")

    ensure_sftp_directory(sftp, REMOTE_DIR)
    clear_sftp_folder(sftp, REMOTE_DIR)

    today = datetime.now().strftime("%Y%m%d")
    generated_files = []

    for short_name, full_table in TABLES.items():
        print(f"\nExporting table: {full_table}")

        df = get_dataframe(engine, full_table)

        filename = f"{short_name}_{today}.csv"
        local_tmp = f"/tmp/{filename}"
        remote_path = f"{REMOTE_DIR}/{filename}"

        df.to_csv(local_tmp, index=False)

        print(f"Uploading {filename} to SFTP...")
        sftp.put(local_tmp, remote_path)
        print("Uploaded:", remote_path)

        generated_files.append(local_tmp)

    sftp.close()

    print("\nSending email with attachments...")
    send_email_reports(generated_files)

    for f in generated_files:
        os.remove(f)

    print("\nPROCESS COMPLETE — Reports exported, uploaded, and emailed successfully!")

# =========================================
# RUN MANUALLY IN JUPYTER
# =========================================

export_reports()
