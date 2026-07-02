import datetime
import logging
import argparse
import time
import requests
import fastf1
import pandas as pd
from pathlib import Path
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from src.utils import config
from src.utils.logger import logging
from src.utils.utility import safe_get, build_session, get_season_rounds, parse_laptime_to_ms, timedelta_to_ms


# =============================================================================
# EXTRACT — weather from FastF1 session
# =============================================================================

def extract_weather(session, year: int,
                    round_num: int, race_name: str,
                    circuit_ref: str) -> pd.DataFrame:
    """
    Extract weather data from a loaded FastF1 race session.

    FastF1 weather_data columns:
        Time          — session timedelta (time into session)
        AirTemp       — air temperature °C
        Humidity      — relative humidity %
        Pressure      — barometric pressure mbar
        Rainfall      — boolean (True = rain detected)
        TrackTemp     — track surface temperature °C
        WindDirection — wind direction in degrees (0–359)
        WindSpeed     — wind speed m/s

    """
    weather = session.weather_data

    if weather is None or (hasattr(weather, "empty") and weather.empty):
        logging.warning(f"  No weather data available for {year} R{round_num:02d}")
        return pd.DataFrame()

    df = weather.copy()

    # ── Rename to match DDL columns ───────────────────────────────────────
    df = df.rename(columns={
        "Time":          "session_time",
        "AirTemp":       "air_temp_c",
        "Humidity":      "humidity_pct",
        "Pressure":      "pressure_mbar",
        "Rainfall":      "rainfall",
        "TrackTemp":     "track_temp_c",
        "WindDirection": "wind_direction_deg",
        "WindSpeed":     "wind_speed_ms",
    })

    # ── Convert session time to ms ────────────────────────────────────────
    if "session_time" in df.columns:
        df["session_time_ms"] = df["session_time"].apply(timedelta_to_ms)
        df.drop(columns=["session_time"], inplace=True)

    # ── Add race context columns ──────────────────────────────────────────
    df["season"]       = year
    df["round_number"] = round_num
    df["race_name"]    = race_name
    df["circuit_ref"]  = circuit_ref

    # ── Column order ──────────────────────────────────────────────────────
    col_order = [
        "season", "round_number", "race_name", "circuit_ref",
        "session_time_ms",
        "air_temp_c", "track_temp_c", "humidity_pct",
        "pressure_mbar", "wind_speed_ms", "wind_direction_deg",
        "rainfall"
    ]
    col_order = [c for c in col_order if c in df.columns]
    df = df[col_order].reset_index(drop=True)

    return df


# =============================================================================
# SINGLE RACE WEATHER FETCH
# =============================================================================

def fetch_race_weather(session: requests.Session, year: int, round_num: int,
                       cache_dir: Path) -> pd.DataFrame:
    """
    Load FastF1 race session for one round and extract weather data.

    FastF1 caches session data to disk — subsequent calls for the same
    race are instant (no API hit).

    Parameters:
        year      : Season year
        round_num : Race round number
        cache_dir : FastF1 cache directory

    Returns:
        DataFrame with weather readings or empty DataFrame if unavailable
    """
    try:
        session = fastf1.get_session(year, round_num, "Q")

        # Load only weather — skip laps, telemetry, messages 
        session.load(
            laps      = False,
            telemetry = False,
            weather   = True,
            messages  = False,
        )

        race_name   = session.event.get("EventName", f"Round {round_num}")
        circuit_ref = session.event.get("Location",  "unknown")

        return extract_weather(session=session, year=year, round_num=round_num, race_name=race_name, circuit_ref=circuit_ref)

    except Exception as e:
        logging.error(f"  FastF1 load failed for {year} R{round_num:02d}: {e}")
        return pd.DataFrame()


# =============================================================================
# MAIN INGESTION FUNCTION
# =============================================================================

def fetch_weather_data(session: requests.Session,
                       schedule_dir: Path,
                       bronze_dir:   Path,
                       cache_dir:    Path,
                       start_year:   int,
                       end_year:     int,
                       force:        bool = False,
                       single_year:  int  = None,
                       single_round: int  = None) -> pd.DataFrame:
    """
    Fetch weather data for all seasons and rounds via FastF1.
    Saves one parquet per race and one combined master file.

    Parameters:
        bronze_dir   : Path to bronze folder
        cache_dir    : Path to FastF1 cache folder
        start_year   : First season to ingest
        end_year     : Last season to ingest
        force        : Re-fetch all races even if files exist
        single_year  : Ingest only this season
        single_round : Ingest only this round within single_year
    """
    bronze_dir = Path(bronze_dir)
    cache_dir  = Path(cache_dir)
    weather_dir = bronze_dir / "weather"
    weather_dir.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)

    # Enable FastF1 cache — avoids re-downloading session data
    fastf1.Cache.enable_cache(str(cache_dir))
    logging.info(f"  FastF1 cache: {cache_dir}")

    master_path  = weather_dir / "all_seasons_weather.parquet"
    current_year = datetime.datetime.now().year
    new_dfs      = []

    # ── Determine year range ──────────────────────────────────────────────
    if single_year:
        year_range = [single_year]
    else:
        year_range = range(start_year, end_year + 1)

    for year in year_range:
        is_current_season = (year == current_year)

        logging.info(f"\n── Season {year} ──────────────────────────────")

        # FastF1 weather is reliable from 2018 onwards
        if year < 2018:
            logging.warning(f"  {year} — FastF1 weather not available pre-2018, skipping")
            continue

        # ── Determine rounds ──────────────────────────────────────────────
        if single_round and single_year:
            rounds = [single_round]
        else:
            rounds = get_season_rounds(schedule_dir=schedule_dir, year=year)

        if not rounds:
            logging.warning(f"  No rounds found for {year} — skipping")
            continue

        logging.info(f"  {len(rounds)} rounds to process")

        for round_num in rounds:
            save_path = weather_dir / f"{year}_R{round_num:02d}_weather.parquet"

            # ── Skip if already fetched ───────────────────────────────────
            if save_path.exists() and save_path.stat().st_size > 0 \
                    and not force and not is_current_season:
                logging.info(f"  R{round_num:02d} already exists — skipping")
                continue

            # ── Fetch ─────────────────────────────────────────────────────
            logging.info(f"  Fetching {year} R{round_num:02d} weather...")
            df_round = fetch_race_weather(session=session, year=year, round_num=round_num, cache_dir=cache_dir)

            if df_round.empty:
                logging.warning(
                    f"  No weather data for {year} R{round_num:02d} — skipping"
                )
                continue

            # ── Save individual race file ─────────────────────────────────
            df_round.to_parquet(save_path, index=False, compression="snappy")
            logging.info(
                f"  Saved {year}_R{round_num:02d}_weather.parquet "
                f"({len(df_round)} rows"
            )
            new_dfs.append(df_round)

        # Pause between seasons
        if not single_year and year < end_year:
            logging.info(f"  Pausing {config.INTER_SEASON_DELAY}s between seasons...")
            time.sleep(config.INTER_SEASON_DELAY)

    # ── Update master file ────────────────────────────────────────────────
    logging.info("\n── Updating master file ─────────────────────")

    if new_dfs:
        new_data = pd.concat(new_dfs, ignore_index=True)

        if master_path.exists():
            master = pd.read_parquet(master_path)

            # Remove current season rows to avoid duplicates
            master = master[master["season"] != current_year]

            master = pd.concat([master, new_data], ignore_index=True)
            master = master.sort_values(
                ["season", "round_number", "session_time_ms"]
            ).reset_index(drop=True)

        else:
            # Build master from all individual files
            logging.info("  Master not found — building from individual files...")
            all_files = sorted(weather_dir.glob("*_R*_weather.parquet"))

            if not all_files:
                logging.warning("  No weather parquet files found.")
                master = pd.DataFrame()
            else:
                master = pd.concat(
                    [pd.read_parquet(f) for f in all_files],
                    ignore_index=True
                ).sort_values(
                    ["season", "round_number", "session_time_ms"]
                ).reset_index(drop=True)

        master.to_parquet(master_path, index=False, compression="snappy")
        logging.info(f"  Master updated — {len(master):,} total rows")

    else: 
        # ── No new data ───────────────────────────────────────────────────────
        if not master_path.exists():
            logging.warning("  No new data and no master file — returning empty | Set FORCE = True.")
            return pd.DataFrame()

        logging.info("  No new seasons fetched — master unchanged, loading from disk")
        master = pd.read_parquet(master_path)  

    return master


# ── Run ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # BUILD SESSION
    SESSION = build_session()

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
   
    print("\nWEATHER DATA:")
    logging.info(df.head())
    print(df.head())
