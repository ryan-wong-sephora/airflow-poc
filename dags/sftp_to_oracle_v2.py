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

# SFTP config
SFTP_HOST = os.getenv("SFTP_HOST")
SFTP_PORT = int(os.getenv("SFTP_PORT", "22"))
SFTP_USER = os.getenv("SFTP_USERNAME")
SFTP_PASS = os.getenv("SFTP_PASSWORD")
SFTP_DIR = os.getenv("SFTP_DIR")

# Oracle config
ORACLE_HOST = os.getenv("ORACLE_HOST")
ORACLE_PORT = os.getenv("ORACLE_PORT")
ORACLE_SERVICE = os.getenv("ORACLE_SERVICE")
ORACLE_USER = os.getenv("ORACLE_USERNAME")
ORACLE_PASS = os.getenv("ORACLE_PASSWORD")
ORACLE_TABLE = os.getenv("ORACLE_TABLE")

# Local
LOCAL_CSV_DIR = os.getenv("LOCAL_CSV_DIR")
INSTANT_CLIENT_PATH = os.getenv("INSTANT_CLIENT_PATH")

DC_CONST = "WDC"
WH_ID_CONST = "W1030"

# Thick mode is required (native network encryption)
oracledb.init_oracle_client(lib_dir=INSTANT_CLIENT_PATH)


def _download_to_local() -> str:
    """
    Download PickPerformance.csv from SFTP to LOCAL_CSV_DIR and return local path.
    This is essentially your original function, just used inside one task.
    """
    os.makedirs(LOCAL_CSV_DIR, exist_ok=True)

    remote_path = f"{SFTP_DIR}/PickPerformance.csv"
    local_path = os.path.join(LOCAL_CSV_DIR, "PickPerformance.csv")

    logger.info("Connecting to SFTP server at %s:%s", SFTP_HOST, SFTP_PORT)
    transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
    transport.connect(username=SFTP_USER, password=SFTP_PASS)
    sftp = paramiko.SFTPClient.from_transport(transport)

    logger.info("Downloading %s to %s", remote_path, local_path)
    t0 = time.perf_counter()
    sftp.get(remote_path, local_path)
    t1 = time.perf_counter()
    logger.info("Download completed in %.2f seconds", t1 - t0)

    sftp.close()
    transport.close()

    return local_path


def _load_and_transform(csv_path: str) -> pd.DataFrame:
    """
    Read CSV from local path and apply all transformations.
    Returns cleaned DataFrame ready for Oracle insert.
    """
    logger.info("Reading CSV from %s", csv_path)
    t0 = time.perf_counter()
    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    t1 = time.perf_counter()
    logger.info("CSV read completed in %.2f seconds", t1 - t0)

    if df is None or df.empty:
        raise RuntimeError(f"CSV at {csv_path} is empty or failed to parse")

    expected_cols = {
        "Cur. qty.",
        "Article number",
        "PIN Code",
        "Pick station",
        "Start time",
        "End time",
        "Pick duration [s]",
    }
    missing = expected_cols - set(df.columns)
    if missing:
        logger.error(
            "Missing required columns: %s. Found columns: %s",
            missing,
            df.columns.tolist(),
        )
        raise ValueError(
            f"Missing expected columns in CSV: {missing}. "
            f"Found: {df.columns.tolist()}"
        )

    logger.info("Loaded %d rows from CSV", len(df))

    # Transformations
    df["dc_name"] = DC_CONST
    df["wh_id"] = WH_ID_CONST
    df["current_qty"] = pd.to_numeric(df["Cur. qty."], errors="coerce")
    df["prtnum"] = df["Article number"]
    df["user_id"] = df["PIN Code"].fillna("").str.strip().str.upper()
    df["pick_station"] = df["Article number"]

    df["start_time"] = pd.to_datetime(
        df["Start time"], format="%m/%d/%Y %I:%M:%S %p", errors="coerce"
    )
    df["end_time"] = pd.to_datetime(
        df["End time"], format="%m/%d/%Y %I:%M:%S %p", errors="coerce"
    )

    business_date = (datetime.now() - timedelta(days=1)).date()
    df["business_date"] = business_date

    df["pick_duration"] = pd.to_numeric(
        df["Pick duration [s]"], errors="coerce"
    )

    insert_df = df[
        [
            "dc_name",
            "wh_id",
            "current_qty",
            "prtnum",
            "user_id",
            "pick_station",
            "start_time",
            "end_time",
            "pick_duration",
            "business_date",
        ]
    ].copy()

    initial_count = len(insert_df)
    insert_df = insert_df.dropna(
        subset=["current_qty", "pick_duration", "start_time", "end_time"]
    )
    dropped_count = initial_count - len(insert_df)

    if dropped_count > 0:
        logger.warning(
            "Dropped %d rows with invalid data (qty/duration/date)",
            dropped_count,
        )

    logger.info(
        "Data validation complete: %d rows valid, %d rows dropped",
        len(insert_df),
        dropped_count,
    )

    # NaN -> None for Oracle
    insert_df = insert_df.where(pd.notnull(insert_df), None)

    return insert_df


def _load_into_oracle_batched(insert_df: pd.DataFrame, batch_size: int = 20000) -> int:
    """
    Insert into Oracle in batches using executemany, itertuples, and positional binds.
    """
    logger.info(
        "Connecting to Oracle DB at %s:%s/%s", ORACLE_HOST, ORACLE_PORT, ORACLE_SERVICE
    )
    conn = oracledb.connect(
        user=ORACLE_USER,
        password=ORACLE_PASS,
        dsn=f"{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}",
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

    total_rows = len(insert_df)
    inserted_rows = 0

    t0_all = time.perf_counter()

    for start in range(0, total_rows, batch_size):
        end = min(start + batch_size, total_rows)
        batch_df = insert_df.iloc[start:end]

        # Phase timing for visibility
        t0 = time.perf_counter()
        batch_data = [
            tuple(row)
            for row in batch_df.itertuples(index=False, name=None)
        ]
        t1 = time.perf_counter()

        cursor.bindarraysize = len(batch_data)
        cursor.executemany(insert_sql, batch_data)
        t2 = time.perf_counter()

        conn.commit()
        t3 = time.perf_counter()

        inserted_rows += len(batch_data)
        logger.info(
            "Batch %d-%d: convert=%.2fs, executemany=%.2fs, commit=%.2fs (rows=%d)",
            start,
            end,
            t1 - t0,
            t2 - t1,
            t3 - t2,
            len(batch_data),
        )

    conn.close()
    t1_all = time.perf_counter()

    logger.info(
        "Total Oracle insert time (including commits): %.2f seconds for %d rows",
        t1_all - t0_all,
        inserted_rows,
    )

    return inserted_rows


with DAG(
    dag_id="sftp_to_oracle_pick_performance_v2",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    description="POC for ETL process from SFTP -> Oracle (single-task, optimized, batched)",
):

    @task
    def run_pick_performance_etl() -> dict:
        etl_start = time.perf_counter()

        csv_path = _download_to_local()
        insert_df = _load_and_transform(csv_path)
        inserted_rows = _load_into_oracle_batched(insert_df, batch_size=5000)

        etl_end = time.perf_counter()
        elapsed = etl_end - etl_start

        logger.info(
            "End-to-end ETL complete: %d rows inserted in %.2f seconds",
            inserted_rows,
            elapsed,
        )

        return {
            "rows_inserted": inserted_rows,
            "etl_seconds": elapsed,
        }

    run_pick_performance_etl()
