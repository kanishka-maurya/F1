"""
==================================================================================
F1 Bronze Layer — Parquet Load Script
==================================================================================
This script:
    - Loads data from source Parquet files into Bronze layer tables
    - Captures total batch and individual table load durations
    - Includes structured try/except error handling for reliability
    - Uses informative logging for real-time progress tracking
    - Truncates each table before loading (idempotent)
    - Mirrors the SQL Server stored procedure pattern in Python/PostgreSQL
==================================================================================
"""

import pandas as pd
from sqlalchemy import create_engine, text
import time
from urllib.parse import quote_plus
import logging
import os
from pathlib import Path
from dotenv import load_dotenv

# ─── Environment Variable setup ────────────────────────────────────────────────────────────
load_dotenv()
db_password = os.getenv("POSTGRES_PASSWORD")
bronze_path = Path(os.getenv("BRONZE_PATH"))


# ─── Logging setup ────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


# ─── Sub-Directories ───────────────────────────────────────────────────────────────────
ENTITIES_PATH = bronze_path / "entities"   # circuits, drivers, constructors
EVENTS_PATH   = bronze_path  

# ─── Config ───────────────────────────────────────────────────────────────────

DB_CONFIG = {
    "host"    : "localhost",
    "port"    : 5432,
    "database": "Formula1",
    "user"    : "postgres",
    "password": db_password        # ← change this
}

# Root folder where your parquet files live
BRONZE_PATH = Path(bronze_path)

# Map: PostgreSQL table name → parquet file name
PARQUET_MAP = {
    # entities
    "circuits": ENTITIES_PATH / "circuits.parquet",
    "constructors": ENTITIES_PATH / "constructors.parquet",
    "drivers": ENTITIES_PATH / "drivers.parquet",

    # event folders
    "laps": EVENTS_PATH / "laps" / "all_seasons_laps.parquet",
    "pitstops": EVENTS_PATH / "pit_stops" / "all_seasons_pit_stops.parquet",
    "qualifying": EVENTS_PATH / "qualifying" / "all_seasons_qualifying.parquet",
    "results": EVENTS_PATH / "results" / "all_seasons_results.parquet",
    "schedule": EVENTS_PATH / "schedule" / "all_seasons_schedule.parquet",
    "telemetry": EVENTS_PATH / "telemetry" / "all_seasons_telemetry.parquet",
    "weather": EVENTS_PATH / "weather" / "all_seasons_weather.parquet",
}


# ─── Database connection ───────────────────────────────────────────────────────

def get_engine():
    password = quote_plus(DB_CONFIG["password"])
    url = (
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{password}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    return create_engine(url)


# ─── Single table loader ───────────────────────────────────────────────────────

def load_table(engine, table_name, parquet_file):
    """
    Truncates bronze.<table_name> and loads from parquet_file.
    Returns duration in seconds.
    """

    logger.info(f"--------------------------------------------")
    logger.info(f">>>> Truncating table: bronze.{table_name}")

    with engine.connect() as conn:
        conn.execute(text(f"TRUNCATE TABLE bronze.{table_name}"))
        conn.commit()

    logger.info(f"Inserting data into: bronze.{table_name}")

    # Read parquet
    df = pd.read_parquet(parquet_file)

    start_time = time.time()

    max_params = 32767  # conservative limit
    chunk = max(1, max_params // len(df.columns))

    # Load into PostgreSQL
    df.to_sql(
        name      = table_name,
        con       = engine,
        schema    = "bronze",
        if_exists = "append",
        index     = False,
        method    = "multi",
        chunksize = chunk
    )

    duration = round(time.time() - start_time, 2)

    logger.info(f"Rows loaded   : {len(df):,}")
    logger.info(f"Load duration : {duration} seconds")
    logger.info(f"--------------------------------------------")

    return duration


# ─── Main load procedure ───────────────────────────────────────────────────────

def load_bronze():
    """
    Main entry point. Loads all bronze tables from parquet files.
    Equivalent of the SQL Server stored procedure bronze.load_bronze.
    """

    logger.info("=======================================================")
    logger.info("Starting bronze load procedure...")
    logger.info("=======================================================")

    engine = get_engine()
    batch_start     = time.time()
    table_durations = {}
    failed_tables   = []

    for table_name, parquet_path in PARQUET_MAP.items():

        # Check file exists before attempting load
        if not parquet_path.exists():
            logger.warning(f"File not found, skipping : {parquet_path}")
            failed_tables.append(table_name)
            continue

        try:
            duration = load_table(engine, table_name, parquet_path)
            table_durations[table_name] = duration

        except Exception as e:
            logger.error("=======================================================")
            logger.error(f"Error loading bronze.{table_name}")
            logger.error("=======================================================")
            logger.error(f"Error type    : {type(e).__name__}")
            # Truncate: SQLAlchemy appends the full SQL statement + every
            # bound parameter to str(e), which can be tens of thousands of
            # characters for large batch inserts and buries the actual
            # error reason past the terminal's scrollback buffer.
            error_text = str(e)
            logger.error(f"Error message : {error_text[:500]}")
            failed_tables.append(table_name)
            # Continue loading remaining tables even if one fails
            continue

    # ─── Batch summary ────────────────────────────────────────────────────────

    batch_duration = round(time.time() - batch_start, 2)

    logger.info("=======================================================")
    logger.info("Bronze load complete — Summary")
    logger.info("=======================================================")

    for table, dur in table_durations.items():
        logger.info(f"  bronze.{table:<20} {dur:>6} seconds")

    if failed_tables:
        logger.warning("-------------------------------------------------------")
        logger.warning(f"Failed tables ({len(failed_tables)}) : {failed_tables}")
        logger.warning("-------------------------------------------------------")

    logger.info("=======================================================")
    logger.info(f"Total batch duration  : {batch_duration} seconds")
    logger.info(f"Tables loaded         : {len(table_durations)}/{len(PARQUET_MAP)}")
    logger.info("=======================================================")


# ─── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    load_bronze()