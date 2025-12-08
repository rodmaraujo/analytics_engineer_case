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

# ---- Mailtrap SMTP (REAL WORKING EMAIL SERVER) ----
SMTP_HOST = "sandbox.smtp.mailtrap.io"
SMTP_PORT = 587               # 25, 465, 587 or 2525 all work
SMTP_USER = "94114d3b144d33"
SMTP_PASS = "9cb8efdd8d2657"
FROM_EMAIL = "reports@test.com"   # any email works in Mailtrap

# ---- TestMail inbox (receives the emails) ----
TO_EMAIL = "rodrigomartinsaraujo@gmail.com"

# ---- Tables to export ----
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
    transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
    transport.connect(username=SFTP_USER, password=SFTP_PASS)
    return paramiko.SFTPClient.from_transport(transport)


def ensure_sftp_directory(sftp, directory):
    try:
        sftp.listdir(directory)
    except IOError:
        print(f"📁 Creating missing directory: {directory}")
        sftp.mkdir(directory)


def clear_sftp_folder(sftp, directory):
    try:
        files = sftp.listdir(directory)
        for f in files:
            full_path = f"{directory}/{f}"
            print(f"🗑 Removing old file: {full_path}")
            sftp.remove(full_path)
    except IOError:
        print("Directory empty or inaccessible.")

# =========================================
# POSTGRES FUNCTIONS
# =========================================

def get_dataframe(engine, table):
    return pd.read_sql(f"SELECT * FROM {table}", engine)

# =========================================
# EMAIL FUNCTION (SMTP → TestMail)
# =========================================

def send_email_reports(files_to_attach):
    print("\n📧 Sending email with attached reports to TestMail inbox:", TO_EMAIL)

    msg = MIMEMultipart()
    msg["From"] = FROM_EMAIL
    msg["To"] = TO_EMAIL
    msg["Subject"] = "Automated Reports Delivery"

    msg.attach(MIMEText("Attached are the latest exported reports.", "plain"))

    for file_path in files_to_attach:
        filename = os.path.basename(file_path)
        print(f"📎 Attaching: {filename}")

        with open(file_path, "rb") as f:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", f'attachment; filename="{filename}"')
            msg.attach(part)

    # Send via Mailtrap SMTP
    server = smtplib.SMTP(SMTP_HOST, SMTP_PORT)
    server.starttls()
    server.login(SMTP_USER, SMTP_PASS)
    server.sendmail(FROM_EMAIL, [TO_EMAIL], msg.as_string())
    server.quit()

    print("✔️ Email successfully sent to TestMail inbox.")

# =========================================
# MAIN EXPORT PROCESS
# =========================================

def export_reports():

    print("\n🔌 Connecting to PostgreSQL...")
    engine = create_engine(
        f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    )
    print("Connected to PostgreSQL.")

    print("\n🔌 Connecting to SFTP...")
    sftp = sftp_connect()
    print("Connected to SFTP.")

    ensure_sftp_directory(sftp, REMOTE_DIR)
    clear_sftp_folder(sftp, REMOTE_DIR)

    today_str = datetime.now().strftime("%Y%m%d")
    generated_files = []

    for short_name, full_table in TABLES.items():
        print(f"\n📄 Exporting table: {full_table}")

        df = get_dataframe(engine, full_table)

        filename = f"{short_name}_{today_str}.csv"
        local_tmp = f"/tmp/{filename}"
        remote_path = f"{REMOTE_DIR}/{filename}"

        df.to_csv(local_tmp, index=False)

        print(f"⬆️ Uploading {filename} to SFTP...")
        sftp.put(local_tmp, remote_path)
        print(f"✔️ Uploaded: {remote_path}")

        generated_files.append(local_tmp)

    sftp.close()

    print("\n📬 Sending reports via email...")
    send_email_reports(generated_files)

    for f in generated_files:
        os.remove(f)

    print("\n🏁 EXPORT COMPLETE — Reports exported, uploaded and emailed successfully.\n")

# =========================================
# RUN
# =========================================

if __name__ == "__main__":
    export_reports()
