import pandas as pd
import numpy as np
import logging
import pyodbc
import paramiko
from datetime import datetime
from io import StringIO

# === SFTP Credentials ===
sftp_host = "****"
sftp_port = 22
sftp_user = "****"
sftp_password = "****"
remote_file_path = "/incoming/Reporting/Dashboard/Employee_MasterReport.csv"

# === Setup Logging ===
logging.basicConfig(
    filename="import_log.txt",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logging.info("=== Starting empmaster import and transform script ===")

# === Fetch CSV from SFTP ===
logging.info("Connecting to SFTP to download file...")
try:
    transport = paramiko.Transport((sftp_host, sftp_port))
    transport.connect(username=sftp_user, password=sftp_password)
    sftp = paramiko.SFTPClient.from_transport(transport)

    with sftp.open(remote_file_path, 'r') as remote_file:
        csv_data = remote_file.read().decode("utf-8")
        df = pd.read_csv(StringIO(csv_data))

    sftp.close()
    transport.close()
    logging.info(f"✅ Successfully downloaded and read CSV from {remote_file_path}")
    logging.info(f"Initial DataFrame shape: {df.shape}")
except Exception as e:
    logging.error(f"❌ SFTP download failed: {e}")
    raise SystemExit("Aborting due to SFTP error.")

# === Updated Data Transformation Logic Starts ===
from datetime import timedelta

final_columns = [
    "UserID", "Business", "FullName", "Event Date", "ContractType", "EmployeeGroup", "SeparationReason",
    "ESG", "ESGBand", "ESGLevel", "EventReason", "Gender", "CandidateHired", "TerminationType",
    "Candidate Hire Date", "Status", "LastModified", "Division", "Dep_name", "DOJ", "DOB", "EventDate",
    "TerminationDate", "RetirementDate", "Age", "Current_exp", "Age_Bins", "Age_Bins_EHS",
    "Service_Bins", "Infancy_exp", "Infancy", "Emp_Category", "Hiring_Status",
    "Department", "Personal Area", "Personal Sub Area"
]

column_mapping = {
    "User/Employee ID": "UserID",
    "Business": "Business",
    "Full Name": "FullName",
    "Event Date": "Event Date",
    "Contract Type": "ContractType",
    "Employee Group": "EmployeeGroup",
    "Separation Reason": "SeparationReason",
    "Employee Sub Group": "ESG",
    "ESG Band": "ESGBand",
    "ESG Level": "ESGLevel",
    "Event Reason": "EventReason",
    "Gender": "Gender",
    "Employment Details Termination Type": "TerminationType",
    "Employment Details Hire Date": "Candidate Hire Date",
    "Employee Status": "Status",
    "Last Modified On": "LastModified",
    "Unit": "Division",
    "Department": "Dep_name",
    "Date Of Birth": "DOB",
    "Position Entry Date": "DOJ",
    "Employment Details Termination Date": "TerminationDate",
    "Retirement Date": "RetirementDate",
    "Department": "Department",
    "Personal Area": "Personal Area",
    "Personal Sub Area": "Personal Sub Area",
    "Designation": "Designation"
}

new_df = pd.DataFrame(columns=final_columns)

for old_col, new_col in column_mapping.items():
    if old_col in df.columns:
        new_df[new_col] = df[old_col]

date_cols = ["DOB", "DOJ", "Event Date", "TerminationDate", "RetirementDate", "LastModified"]
for col in date_cols:
    if col in new_df.columns:
        new_df[col] = pd.to_datetime(new_df[col], errors="coerce")

new_df["EventDate"] = new_df["Event Date"]
new_df["CandidateHired"] = new_df["DOJ"].notnull().map({True: "TRUE", False: "FALSE"})

today = pd.Timestamp.today()
new_df["Age"] = new_df["DOB"].apply(lambda x: (today.year - x.year - ((today.month, today.day) < (x.month, x.day))) if pd.notnull(x) else np.nan)
new_df["Current_exp"] = new_df["DOJ"].apply(lambda x: (today.year - x.year - ((today.month, today.day) < (x.month, x.day))) if pd.notnull(x) else np.nan)

new_df["Age_Bins"] = pd.cut(
    new_df["Age"],
    bins=range(0, 101, 10),
    right=False,
    labels=[f"{i}-{i+10}" for i in range(0, 100, 10)]
)

def age_bins_ehs(age):
    if pd.isna(age): return np.nan
    elif age < 30: return "<30"
    elif 30 <= age <= 50: return "30-50"
    else: return ">50"

new_df["Age_Bins_EHS"] = new_df["Age"].apply(age_bins_ehs)

new_df["Service_Bins"] = pd.cut(
    new_df["Current_exp"],
    bins=range(0, 51, 5),
    right=False,
    labels=[f"{i}-{i+5}" for i in range(0, 50, 5)]
)

new_df["Hiring_Status"] = "RCM"
new_df["Emp_Category"] = new_df["ContractType"].apply(lambda x: "Officer" if str(x).strip().lower() == "officer" else "Non-Officer")

new_df["Infancy_exp"] = new_df.apply(
    lambda row: row["TerminationDate"].year - row["DOJ"].year if pd.notnull(row["TerminationDate"]) and pd.notnull(row["DOJ"]) else np.nan,
    axis=1
)

def classify_infancy(row):
    if pd.isnull(row["Infancy_exp"]) or pd.isnull(row["EmployeeGroup"]): return np.nan
    if row["Infancy_exp"] < 2:
        if row["EmployeeGroup"].strip().lower() == "trainee":
            return "Trainee Attrition"
        else:
            return "Infancy Attrition"
    return np.nan

new_df["Infancy"] = new_df.apply(classify_infancy, axis=1)

business_group_map = {
    "11000413-SBG": "BioSeed", "11000414-BRI": "BioSeed", "11000407-Chemicals": "Chemical",
    "11000409-Corporate": "Corporate", "11000417-Hydro Business": "Corporate", "11000418-DCM Shriram Foundation": "Corporate",
    "11000408-Fenesta": "Fenesta", "10004129-Fertiliser": "KOTA", "10004130-Plastics": "KOTA",
    "10004131-Power": "KOTA", "10004132-Common Kota": "KOTA", "10004133-Cement": "KOTA",
    "11000416-Shriram Polytech": "KOTA", "Shriram Farm Solutions": "KOTA", "11000411-Hariyali": "KOTA",
    "11000419-Shriram AgSmart Limited": "KOTA", "11000415-SGFL": "KOTA", "11000416-Shriram Axiall": "KOTA",
    "11000410-DCM Shriram Ltd - Sugar": "Sugar", "11000410-DSCL Sugar": "Sugar",
    "11000420-Shriram Bio Enchem Limited": "Sugar", "11000412-Shriram Farm Solutions": "SFS"
}

new_df["BusinessGroup"] = new_df["Business"].map(business_group_map)
new_df = new_df.where(pd.notnull(new_df), None)

logging.info(f"✅ Final cleaned DataFrame shape: {new_df.shape}")
null_counts = new_df.isnull().sum()
logging.info("🧹 Null counts per column after transformation:")
logging.info(null_counts.to_string())

# === SQL Server Setup ===
conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=*****;"
    "DATABASE=****;"
    "UID=****;"
    "PWD=****"
)
table_name = "empmaster"

def clean_value(val):
    if pd.isna(val) or str(val).strip().lower() in ['nan', 'nat', '']:
        return None
    if isinstance(val, str):
        for fmt in ("%m/%d/%Y", "%d-%m-%Y", "%Y-%m-%d", "%m/%d/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(val.strip(), fmt).strftime('%Y-%m-%d')
            except:
                continue
    return val

def insert_into_table(df, conn, table_name):
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        existing = cursor.fetchone()[0]
        logging.info(f"Existing rows in {table_name}: {existing}")

        columns = ", ".join(f"[{col}]" for col in df.columns)
        placeholders = ", ".join("?" for _ in df.columns)
        insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        inserted = 0
        total_rows = len(df)

        # Only insert rows after the existing count
        for idx in range(existing, total_rows):
            row = df.iloc[idx]
            try:
                cleaned_row = [clean_value(val) for val in row.tolist()]
                cursor.execute(insert_sql, cleaned_row)
                inserted += 1
            except Exception as e1:
                logging.warning(f"⚠ Retry with nulls for row {idx} due to error: {e1}")
                try:
                    fallback_row = [None] * len(row)
                    cursor.execute(insert_sql, fallback_row)
                    inserted += 1
                except Exception as e2:
                    logging.error(f"❌ Error inserting row {idx}: {row.to_dict()} | Error: {e2}")
            if inserted % 10 == 0:
                logging.info(f"Inserted {inserted} rows so far...")

        conn.commit()
        logging.info(f"✅ Successfully inserted {inserted} new rows.")
    except Exception as e:
        logging.error(f"❌ Fatal error during import: {e}")
    finally:
        cursor.close()


# === Execute Import ===
try:
    logging.info("Connecting to SQL Server...")
    conn = pyodbc.connect(conn_str)
    logging.info("✅ Connected to SQL Server.")
    insert_into_table(new_df, conn, table_name)
    conn.close()
except Exception as e:
    logging.error(f"❌ Connection or execution failed: {e}")

logging.info("=== empmaster import and transform script finished ===")
