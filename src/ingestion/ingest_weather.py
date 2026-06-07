"""
Weather Data Ingestion — FastF1 → Bronze Parquet
================================================
Source   : FastF1 session.weather_data
Coverage : 2018 – 2025 (FastF1 weather data reliable from 2018)

Note on source:
    Weather data is NOT available from Jolpica/Ergast API.
    FastF1 provides weather readings recorded during the race session
    at ~1 minute intervals from official F1 timing feeds.

What this data contains:
    - Air temperature (°C)
    - Track temperature (°C)
    - Humidity (%)
    - Pressure (mbar)
    - Wind speed (m/s) and direction (degrees)
    - Rainfall (boolean)
    All recorded at session_time intervals during the race.

Output:
    bronze/weather/
    ├── 2018_R01_weather.parquet      (~80–120 rows per race)
    ├── 2018_R02_weather.parquet
    ├── ...
    ├── 2025_R24_weather.parquet
    └── all_seasons_weather.parquet   ← combined master file

Usage:
    python -m src.ingestion.ingest_weather
    python -m src.ingestion.ingest_weather --force
    python -m src.ingestion.ingest_weather --year 2024
    python -m src.ingestion.ingest_weather --year 2024 --round 1
"""

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


BASE_URL           = "https://api.jolpi.ca/ergast/f1"
RATE_LIMIT_DELAY   = 0.8
INTER_SEASON_DELAY = 3.0


# =============================================================================
# SESSION WITH RETRY + BACKOFF  (for schedule fetch only)
# =============================================================================

def _build_session() -> requests.Session:
    session = requests.Session()
    retry   = Retry(
        total                      = 5,
        backoff_factor             = 2,
        status_forcelist           = [429, 500, 502, 503, 504],
        allowed_methods            = ["GET"],
        respect_retry_after_header = True,
        raise_on_status            = False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://",  adapter)
    return session


SESSION = _build_session()


# =============================================================================
# SAFE HTTP GET
# =============================================================================

def _safe_get(url: str, timeout: int = 30) -> requests.Response:
    time.sleep(RATE_LIMIT_DELAY)
    while True:
        response = SESSION.get(url, timeout=timeout)
        if response.status_code == 429:
            wait = int(response.headers.get("Retry-After", 10))
            logging.warning(f"  429 received — waiting {wait}s...")
            time.sleep(wait)
            continue
        response.raise_for_status()
        return response


# =============================================================================
# HELPERS
# =============================================================================

def get_season_rounds(year: int) -> list[int]:
    """Fetch round numbers for a season from Jolpica."""
    url = f"{BASE_URL}/{year}/races.json?limit=30"
    try:
        response = _safe_get(url)
        races    = response.json()["MRData"]["RaceTable"]["Races"]
        return [int(r["round"]) for r in races]
    except Exception as e:
        logging.error(f"  Could not fetch rounds for {year}: {e}")
        return []


def timedelta_to_ms(td) -> float | None:
    """Convert pandas Timedelta to milliseconds. Returns None if NaT."""
    try:
        if pd.isnull(td):
            return None
        return round(td.total_seconds() * 1000, 1)
    except Exception:
        return None


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

    We add:
        session_time_ms — Time converted to milliseconds for DB storage
        is_wet_session  — True if ANY rainfall reading is True in this session
        avg_track_temp  — average track temp for the session (summary feature)
        avg_air_temp    — average air temp for the session
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

    # ── Type casting ──────────────────────────────────────────────────────
    df["rainfall"]          = df["rainfall"].astype(bool)
    df["air_temp_c"]        = pd.to_numeric(df["air_temp_c"],        errors="coerce")
    df["track_temp_c"]      = pd.to_numeric(df["track_temp_c"],      errors="coerce")
    df["humidity_pct"]      = pd.to_numeric(df["humidity_pct"],      errors="coerce")
    df["pressure_mbar"]     = pd.to_numeric(df["pressure_mbar"],     errors="coerce")
    df["wind_speed_ms"]     = pd.to_numeric(df["wind_speed_ms"],     errors="coerce")
    df["wind_direction_deg"]= pd.to_numeric(df["wind_direction_deg"],errors="coerce")

    # ── Session-level summary features ───────────────────────────────────
    # These are constant per race — useful for ML without needing
    # to aggregate later during feature engineering
    df["is_wet_session"] = df["rainfall"].any()
    df["avg_track_temp"] = round(df["track_temp_c"].mean(), 2)
    df["avg_air_temp"]   = round(df["air_temp_c"].mean(),   2)
    df["max_track_temp"] = round(df["track_temp_c"].max(),  2)
    df["min_track_temp"] = round(df["track_temp_c"].min(),  2)

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
        "rainfall",
        "is_wet_session", "avg_track_temp", "avg_air_temp",
        "max_track_temp", "min_track_temp",
    ]
    col_order = [c for c in col_order if c in df.columns]
    df = df[col_order].reset_index(drop=True)

    return df


# =============================================================================
# SINGLE RACE WEATHER FETCH
# =============================================================================

def fetch_race_weather(year: int, round_num: int,
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
        session = fastf1.get_session(year, round_num, "R")

        # Load only weather — skip laps, telemetry, messages (faster)
        session.load(
            laps      = False,
            telemetry = False,
            weather   = True,
            messages  = False,
        )

        race_name   = session.event.get("EventName", f"Round {round_num}")
        circuit_ref = session.event.get("Location",  "unknown")

        return extract_weather(session, year, round_num, race_name, circuit_ref)

    except Exception as e:
        logging.error(f"  FastF1 load failed for {year} R{round_num:02d}: {e}")
        return pd.DataFrame()


# =============================================================================
# MAIN INGESTION FUNCTION
# =============================================================================

def fetch_weather_data(bronze_dir:   Path,
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
            rounds = get_season_rounds(year)

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
            df_round = fetch_race_weather(year, round_num, cache_dir)

            if df_round.empty:
                logging.warning(
                    f"  No weather data for {year} R{round_num:02d} — skipping"
                )
                continue

            # ── Save individual race file ─────────────────────────────────
            df_round.to_parquet(save_path, index=False, compression="snappy")
            logging.info(
                f"  Saved {year}_R{round_num:02d}_weather.parquet "
                f"({len(df_round)} rows | "
                f"wet={df_round['is_wet_session'].iloc[0]} | "
                f"avg_track={df_round['avg_track_temp'].iloc[0]}°C)"
            )
            new_dfs.append(df_round)

        # Pause between seasons
        if not single_year and year < end_year:
            logging.info(f"  Pausing {INTER_SEASON_DELAY}s between seasons...")
            time.sleep(INTER_SEASON_DELAY)

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
        logging.info("  No new races fetched — loading master from disk")
        master = pd.read_parquet(master_path)

    logging.info(f"\n{'='*50}")
    logging.info(f"DONE — {len(master):,} total weather rows")
    logging.info(f"{'='*50}")

    return master


# =============================================================================
# CLI
# =============================================================================

if __name__ == "__main__":

    BRONZE_DIR = Path(r'C:\Users\Asus\Desktop\Formula1\data\bronze')
    CACHE_DIR  = Path(config.CACHE_DIR)

    df = fetch_weather_data(
        bronze_dir   = BRONZE_DIR,
        cache_dir    = CACHE_DIR,
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
    print(f"\nSample:\n{df.head()}")