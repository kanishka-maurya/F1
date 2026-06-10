from pathlib import Path
import os 
from src.utils import config
from src.utils.logger import logging
from src.utils.utility import build_session
from src.ingestion.ingest_entities import fetch_all_entities
from src.ingestion.ingest_laps import fetch_lap_data
from src.ingestion.ingest_pitstop import fetch_pit_stops
from src.ingestion.ingest_qualifying import fetch_qualifying_results
from src.ingestion.ingest_results import fetch_race_results
from src.ingestion.ingest_schedule import fetch_race_schedule
from src.ingestion.ingest_telemetry import fetch_telemetry
from src.ingestion.ingest_weather import fetch_weather_data


os.makedirs(config.DATA_DIR, exist_ok=True)

# BUILD SESSION
SESSION = build_session()


logging.info('─────────────────────────────── INGESTING ENTITIES DATA.... ───────────────────────────────')
logging.info("")
# ─────────────────────────────────────── ingesting all entities ───────────────────────────────────────
df = fetch_all_entities(
                            bronze_dir=config.BRONZE_DIR,
                            force=config.FORCE,
                            session= SESSION,
                            timeout=config.TIMEOUT)
 
print("\nDrivers:")
print(df["drivers"].head())

print("\nConstructors:")
print(df["constructors"].head())

print("\nCircuits:")
print(df["circuits"].head())
logging.info("")
logging.info('─────────────────────────────── ENTITIES DATA INGESTED ! ───────────────────────────────')


logging.info('─────────────────────────────── INGESTING SCHEDULE.... ───────────────────────────────')
logging.info("")
# ─────────────────────────────────────── ingesting schedule data ───────────────────────────────────────
df = fetch_race_schedule(
                        session     = SESSION,
                        timeout     = config.TIMEOUT,
                        bronze_dir  = config.BRONZE_DIR, 
                        start_year  = config.START_YEAR, 
                        end_year    = config.END_YEAR, 
                        force       = config.FORCE
                        )
 
print("\nRACE SCHEDULE DATA:")
logging.info(df.head())
print(df.head())
logging.info("")
logging.info('─────────────────────────────── SCHEDULE INGESTED ! ───────────────────────────────')


logging.info('─────────────────────────────── INGESTING LAP DATA.... ───────────────────────────────')
logging.info("")
# ─────────────────────────────────────── ingesting laps data ───────────────────────────────────────
df = fetch_lap_data(
        session      = SESSION,
        schedule_dir = config.BRONZE_DIR / "schedule",
        bronze_dir   = config.BRONZE_DIR,
        start_year   = config.START_YEAR,
        end_year     = config.END_YEAR,
        timeout      = config.TIMEOUT,
        force        = config.FORCE,
        single_year  = config.YEAR,
        single_round = config.ROUND,
    )

print(f"\nShape    : {df.shape}")
print(f"Seasons  : {sorted(df['season'].unique())}")
print(f"Columns  : {list(df.columns)}")

print("\nLAPS DATA:")
logging.info(df.head())
print(df.head())
logging.info("")
logging.info('─────────────────────────────── LAP DATA INGESTED ! ───────────────────────────────')


logging.info('─────────────────────────────── INGESTING PIT STOP DATA.... ───────────────────────────────')
logging.info("")
# ─────────────────────────────────────── ingesting pit stops data ───────────────────────────────────────
df = fetch_pit_stops(
        session      = SESSION,
        schedule_dir = config.BRONZE_DIR / "schedule", 
        bronze_dir   = config.BRONZE_DIR,
        start_year   = config.START_YEAR,
        end_year     = config.END_YEAR,
        timeout      = config.TIMEOUT,
        force        = config.FORCE,
        single_year  = config.YEAR,
        single_round = config.ROUND,
    )

print(f"\nShape         : {df.shape}")
print(f"Seasons       : {sorted(df['season'].unique())}")
print(f"Columns       : {list(df.columns)}")


print("\nPITSTOP DATA:")
logging.info(df.head())
print(df.head())
logging.info("")
logging.info('─────────────────────────────── PIT STOP DATA INGESTED ! ───────────────────────────────')


logging.info('─────────────────────────────── INGESTING QUALIFYING DATA.... ───────────────────────────────')
logging.info("")
# ─────────────────────────────────────── ingesting qualifying data ───────────────────────────────────────
df = fetch_qualifying_results(
        session      = SESSION,
        schedule_dir = config.BRONZE_DIR / "schedule", 
        bronze_dir   = config.BRONZE_DIR,
        start_year   = config.START_YEAR,
        end_year     = config.END_YEAR,
        timeout      = config.TIMEOUT,
        force        = config.FORCE,
    )

print(f"\nShape         : {df.shape}")
print(f"Seasons       : {sorted(df['season'].unique())}")
print(f"Columns       : {list(df.columns)}")

print("\nQUALIFYING DATA:")
logging.info(df.head())
print(df.head())
logging.info("")
logging.info('─────────────────────────────── QUALIFYING DATA INGESTED ! ───────────────────────────────')


logging.info('─────────────────────────────── INGESTING RESULTS DATA.... ───────────────────────────────')
logging.info("")
# ─────────────────────────────────────── ingesting results data ───────────────────────────────────────
df = fetch_race_results(
        session      = SESSION,
        schedule_dir = config.BRONZE_DIR / "schedule", 
        bronze_dir   = config.BRONZE_DIR,
        start_year   = config.START_YEAR,
        end_year     = config.END_YEAR,
        timeout      = config.TIMEOUT,
        force        = config.FORCE,
    )
 
print(f"\nShape       : {df.shape}")
print(f"Seasons     : {sorted(df['season'].unique())}")
print(f"Winners     : {df[df['is_winner']]['driver_ref'].value_counts().head()}")

print("\nRACE RESULTS DATA:")
logging.info(df.head())
print(df.head())
logging.info("")
logging.info('─────────────────────────────── RESULTS DATA INGESTED ! ───────────────────────────────')


logging.info('─────────────────────────────── INGESTING TELEMETRY DATA.... ───────────────────────────────')
logging.info("")
# ─────────────────────────────────────── ingesting telemetry data ───────────────────────────────────────
df = fetch_telemetry(
        session      = SESSION,
        timeout      = config.TIMEOUT,
        schedule_dir = config.BRONZE_DIR / "schedule",
        bronze_dir   = config.BRONZE_DIR,
        cache_dir    = config.CACHE_DIR,
        start_year   = config.START_YEAR,
        end_year     = config.END_YEAR,
        force        = config.FORCE,
        single_year  = config.YEAR,
        single_round = config.ROUND,
    )

print(f"\nShape        : {df.shape}")
print(f"Seasons      : {sorted(df['season'].unique())}")
print(f"Columns      : {list(df.columns)}")

print("\nTELEMETRY DATA:")
logging.info(df.head())
print(df.head())
logging.info("")
logging.info('─────────────────────────────── TELEMETRY DATA INGESTED ! ───────────────────────────────')


logging.info('─────────────────────────────── INGESTING WEATHER DATA.... ───────────────────────────────')
logging.info("")
# ─────────────────────────────────────── ingesting weather data ───────────────────────────────────────
df = fetch_weather_data(
        session      = SESSION,
        schedule_dir = config.BRONZE_DIR / "schedule", 
        bronze_dir   = config.BRONZE_DIR,
        cache_dir    = config.CACHE_DIR,
        start_year   = config.START_YEAR,
        end_year     = config.END_YEAR,
        force        = config.FORCE,
        single_year  = config.YEAR,
        single_round = config.ROUND,
    )

print(f"\nShape      : {df.shape}")
print(f"Seasons    : {sorted(df['season'].unique())}")
print(f"Columns    : {list(df.columns)}")
print(f"Wet races  : {df.drop_duplicates('round_number')['is_wet_session'].sum()}")

print("\nWEATHER DATA:")
logging.info(df.head())
print(df.head())
logging.info("")
logging.info('─────────────────────────────── WEATHER DATA INGESTED ! ───────────────────────────────')