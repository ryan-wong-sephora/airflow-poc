from datetime import datetime, timedelta
import os
import time
import logging

from airflow import DAG
from airflow.sdk import task
from dotenv import load_dotenv
import paramiko
import oracledb
import pandas as pd

logger = logging.getLogger(__name__)

load_dotenv()

SFTP_HOST = os.getenv("SFTP_HOST")
SFTP_PORT = int(os.getenv("SFTP_PORT", "22"))
SFTP_USER = os.getenv("SFTP_USERNAME")
SFTP_PASS = os.getenv("SFTP_PASSWORD")
SFTP_DIR = os.getenv("SFTP_DIR")

ORACLE_HOST = os.getenv("ORACLE_HOST")
ORACLE_PORT = os.getenv("ORACLE_PORT")
ORACLE_SERVICE = os.getenv("ORACLE_SERVICE")
ORACLE_USER = os.getenv("ORACLE_USERNAME")
ORACLE_PASS = os.getenv("ORACLE_PASSWORD")
ORACLE_TABLE = os.getenv("ORACLE_TABLE")

LOCAL_CSV_DIR = os.getenv("LOCAL_CSV_DIR")
INSTANT_CLIENT_PATH = os.getenv("INSTANT_CLIENT_PATH") # For thick connection with Oracle

DC_CONST = "WDC"
WH_ID_CONST = "W1030"

DATE_FORMAT = "MM/DD/YYYY HH:MI:SS AM"

oracledb.init_oracle_client(
    lib_dir=INSTANT_CLIENT_PATH
)

with DAG(
    dag_id="sftp_to_oracle_pick_performance_v1",
    start_date=datetime(2025, 1, 1),
    schedule=None, # manually execute for POC
    catchup=False,
    description="POC for ETL process from SFTP -> Oracle"
):
    @task
    def download_latest_pick_performance() -> str:
        """
        Connects to SFTP and finds the most recent PickPerformance.csv in SFTP.
        Downloads it to LOCAL_CSV_DIR and returns local filepath.
        """
        os.makedirs(LOCAL_CSV_DIR, exist_ok=True)

        logger.info("Connecting to SFTP server at %s:%s", SFTP_HOST, SFTP_PORT)
        transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
        transport.connect(username=SFTP_USER, password=SFTP_PASS)
        sftp = paramiko.SFTPClient.from_transport(transport)

        remote_path = f"{SFTP_DIR}/PickPerformance.csv"
        local_path = os.path.join(LOCAL_CSV_DIR, "PickPerformance.csv")

        logger.info("Downloading %s to %s", remote_path, local_path)

        t0 = time.perf_counter()
        sftp.get(remote_path, local_path)
        t1 = time.perf_counter()

        logger.info("Download completed in %.2f seconds", t1 - t0)

        sftp.close()
        transport.close()

        return local_path
    
    @task
    def transform_and_load(csv_path: str) -> dict:
        """
        Reads csv_path using pandas, maps SFTP columns to Oracle columns with
        required transformations, and inserts into ORACLE_TABLE in batches.
        """
        etl_start = time.perf_counter()

        # Read CSV with pandas
        logger.info("Reading CSV from %s", csv_path)
        df = pd.read_csv(csv_path, encoding="utf-8-sig")

        # Validate expected columns
        expected_cols = {
            "Cur. qty.",
            "Article number",
            "PIN Code",
            "Pick station",
            "Start time",
            "End time",
            "Pick duration [s]"
        }
        missing = expected_cols - set(df.columns)
        if missing:
            logger.error("Missing required columns: %s. Found columns: %s", missing, df.columns.tolist())
            raise ValueError(
                f"Missing expected columns in CSV: {missing}. "
                f"Found: {df.columns.tolist()}"
            )

        logger.info("Loaded %d rows from CSV", len(df))

        # Data transformations using pandas (vectorized operations)
        df["dc_name"] = DC_CONST
        df["wh_id"] = WH_ID_CONST
        df["current_qty"] = pd.to_numeric(df["Cur. qty."], errors="coerce")
        df["prtnum"] = df["Article number"]
        df["user_id"] = df["PIN Code"].fillna("").str.strip().str.upper()
        df["pick_station"] = df["Article number"]

        # Parse dates in Python (much faster than Oracle's TO_DATE)
        df["start_time"] = pd.to_datetime(df["Start time"], format="%m/%d/%Y %I:%M:%S %p", errors="coerce")
        df["end_time"] = pd.to_datetime(df["End time"], format="%m/%d/%Y %I:%M:%S %p", errors="coerce")

        # Calculate business date once (yesterday's date)
        business_date = (datetime.now() - timedelta(days=1)).date()
        df["business_date"] = business_date

        df["pick_duration"] = pd.to_numeric(df["Pick duration [s]"], errors="coerce")

        # Select only the columns we need for insertion
        insert_df = df[[
            "dc_name",
            "wh_id",
            "current_qty",
            "prtnum",
            "user_id",
            "pick_station",
            "start_time",
            "end_time",
            "pick_duration",
            "business_date"
        ]].copy()

        # Drop rows with invalid required fields
        initial_count = len(insert_df)
        insert_df = insert_df.dropna(subset=["current_qty", "pick_duration", "start_time", "end_time"])
        dropped_count = initial_count - len(insert_df)

        if dropped_count > 0:
            logger.warning("Dropped %d rows with invalid data (check quantity, duration, or date fields)", dropped_count)

        logger.info("Data validation complete: %d rows valid, %d rows dropped", len(insert_df), dropped_count)

        logger.info("Connecting to Oracle DB at %s:%s/%s", ORACLE_HOST, ORACLE_PORT, ORACLE_SERVICE)
        conn = oracledb.connect(
            user=ORACLE_USER,
            password=ORACLE_PASS,
            dsn=f"{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}"
        )
        conn.autocommit = False
        cursor = conn.cursor()

        insert_sql = f"""
            INSERT INTO {ORACLE_TABLE} (
                DC_NAME,
                WH_ID,
                CURRENT_QTY,
                PRTNUM,
                USER_ID,
                PICK_STATION,
                START_TIME,
                END_TIME,
                PICK_DURATION,
                BUSINESS_DATE
            )
            VALUES (
                :1, :2, :3, :4, :5, :6, :7, :8, :9, :10
            )
        """

        # Convert DataFrame to list of tuples for executemany
        batch_size = 25000
        inserted_rows = 0

        # Process in batches
        for i in range(0, len(insert_df), batch_size):
            batch_df = insert_df.iloc[i:i + batch_size]

            # Convert to list of tuples
            batch_data = [tuple(x) for x in batch_df.values]

            cursor.executemany(insert_sql, batch_data)
            inserted_rows += len(batch_data)
            # print(f"Processed {inserted_rows} rows so far")

        print(f"Committing {inserted_rows} rows to database...")
        conn.commit()

        etl_end = time.perf_counter()
        elapsed_time = etl_end - etl_start

        cursor.close()
        conn.close()

        print(
            f"Inserted {inserted_rows} rows in {elapsed_time:.2f} seconds"
        )

        return {
            "rows_inserted": inserted_rows,
            "etl_seconds": elapsed_time
        }
    
    csv_path = download_latest_pick_performance()
    transform_and_load(csv_path)

                


        

        
