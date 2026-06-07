"""
Qualifying Results Ingestion — Jolpica API → Bronze Parquet
===========================================================
Endpoint : https://api.jolpi.ca/ergast/f1/{year}/{round}/qualifying.json
Coverage : 2010 – 2025 (all races, all drivers)

What this data contains:
    - Q1, Q2, Q3 lap times per driver per race weekend
    - Grid position (final starting position after penalties)
    - Gap to pole position (computed here)
    - Teammate gap (computed here)

Output:
    bronze/qualifying/
    ├── 2010_R01_qualifying.parquet
    ├── 2010_R02_qualifying.parquet
    ├── ...
    ├── 2025_R24_qualifying.parquet
    └── all_seasons_qualifying.parquet   ← combined master file

Note on data availability:
    - Q1/Q2/Q3 times available from ~2006 onwards
    - Some rounds only have Q1/Q2 if session was wet or cancelled
    - Pre-2003 format was different (single session) — not relevant for 2010+

Usage:
    python -m src.ingestion.ingest_qualifying
    python -m src.ingestion.ingest_qualifying --force
"""

import datetime
import argparse
import time
import requests
import pandas as pd
from src.utils.logger import logging
from pathlib import Path
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from src.utils import config

BASE_URL          = "https://api.jolpi.ca/ergast/f1"
RATE_LIMIT_DELAY  = 1.0   # seconds between every API call
INTER_SEASON_DELAY = 3.0  # extra pause between seasons


# =============================================================================
# SESSION WITH RETRY + BACKOFF
# =============================================================================

def _build_session() -> requests.Session:
    """
    Create a requests.Session with automatic retry + exponential backoff.

    Retry schedule with backoff_factor=2:
        attempt 1 → wait  2 s
        attempt 2 → wait  4 s
        attempt 3 → wait  8 s
        attempt 4 → wait 16 s
        attempt 5 → wait 32 s

    respect_retry_after_header=True means that if the server sends a
    Retry-After header we honour it instead of our own schedule.
    """
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
    """
    GET with:
      - fixed inter-request delay (RATE_LIMIT_DELAY)
      - session-level retry / backoff
      - manual 429 fallback (reads Retry-After header)

    Raises requests.HTTPError on non-2xx after all retries exhausted.
    """
    time.sleep(RATE_LIMIT_DELAY)

    while True:
        response = SESSION.get(url, timeout=timeout)

        if response.status_code == 429:
            wait = int(response.headers.get("Retry-After", 10))
            logging.warning(f"  429 received — waiting {wait}s before retry...")
            time.sleep(wait)
            continue

        response.raise_for_status()
        return response


# =============================================================================
# HELPERS
# =============================================================================

def get_season_rounds(year: int) -> list[int]:
    """
    Fetch list of round numbers for a given season.
    Returns empty list if season not found.
    """
    url = f"{BASE_URL}/{year}/races.json?limit=30"
    try:
        response = _safe_get(url)
        races    = response.json()["MRData"]["RaceTable"]["Races"]
        return [int(r["round"]) for r in races]
    except Exception as e:
        logging.error(f"  Could not fetch rounds for {year}: {e}")
        return []


def parse_laptime_to_ms(laptime_str: str) -> float | None:
    """
    Convert a lap time string to milliseconds.

    Formats handled:
        "1:23.456"  → 83456.0  ms
        "83.456"    → 83456.0  ms  (no minutes part)
        None / ""   → None
    """
    if not laptime_str or pd.isnull(laptime_str):
        return None
    try:
        laptime_str = str(laptime_str).strip()
        if ":" in laptime_str:
            minutes, seconds = laptime_str.split(":")
            total_seconds    = int(minutes) * 60 + float(seconds)
        else:
            total_seconds = float(laptime_str)
        return round(total_seconds * 1000, 1)
    except Exception:
        return None


# =============================================================================
# EXTRACT
# =============================================================================

def extract_qualifying(races: list, year: int, round_num: int) -> pd.DataFrame:
    """
    Extract and flatten qualifying results from API response.
    One row per driver per qualifying session.

    Computes:
        - best_quali_time_ms  : best of Q1/Q2/Q3 in milliseconds
        - gap_to_pole_ms      : delta to fastest qualifier
        - gap_to_teammate_ms  : delta to teammate's best time
        - made_q2, made_q3    : boolean flags for session progression
    """
    if not races:
        return pd.DataFrame()

    race    = races[0]
    records = []

    for r in race.get("QualifyingResults", []):
        driver      = r.get("Driver",      {})
        constructor = r.get("Constructor", {})

        q1_str = r.get("Q1", None)
        q2_str = r.get("Q2", None)
        q3_str = r.get("Q3", None)

        q1_ms  = parse_laptime_to_ms(q1_str)
        q2_ms  = parse_laptime_to_ms(q2_str)
        q3_ms  = parse_laptime_to_ms(q3_str)

        records.append({
            # Race context
            "season":            year,
            "round_number":      round_num,
            "race_name":         race.get("raceName"),
            "circuit_ref":       race.get("Circuit", {}).get("circuitId"),
            "race_date":         race.get("date"),

            # Driver
            "driver_ref":        driver.get("driverId"),
            "driver_code":       driver.get("code"),
            "driver_number":     r.get("number"),

            # Constructor
            "constructor_ref":   constructor.get("constructorId"),

            # Qualifying position
            "quali_position":    r.get("position"),

            # Raw lap time strings — kept for reference
            "q1_time":           q1_str,
            "q2_time":           q2_str,
            "q3_time":           q3_str,

            # Lap times in milliseconds — used for ML features
            "q1_time_ms":        q1_ms,
            "q2_time_ms":        q2_ms,
            "q3_time_ms":        q3_ms,
        })

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)

    # ── Type casting ──────────────────────────────────────────────────────
    df["quali_position"] = pd.to_numeric(df["quali_position"], errors="coerce")
    df["driver_number"]  = pd.to_numeric(df["driver_number"],  errors="coerce")
    df["race_date"]      = pd.to_datetime(df["race_date"], errors="coerce").dt.date

    # ── Best quali time  ──────────────────────────────────────────────────
    df["best_quali_time_ms"] = df["q3_time_ms"].fillna(df["q2_time_ms"]).fillna(df["q1_time_ms"])
    df["gap_to_pole"]= df["best_quali_time_ms"] - df["best_quali_time_ms"].min()
       

    # ── Session progression flags ─────────────────────────────────────────
    df["made_q2"] = df["q2_time_ms"].notna()
    df["made_q3"] = df["q3_time_ms"].notna()

    return df


def fetch_round_qualifying(year: int, round_num: int) -> pd.DataFrame:
    """Fetch qualifying results for a single race round."""
    url = f"{BASE_URL}/{year}/{round_num}/qualifying.json"
    try:
        response = _safe_get(url)
        races    = response.json()["MRData"]["RaceTable"]["Races"]
        return extract_qualifying(races, year, round_num)
    except Exception as e:
        logging.error(f"  API call failed for {year} R{round_num:02d}: {e}")
        return pd.DataFrame()


# =============================================================================
# MAIN INGESTION FUNCTION
# =============================================================================

def fetch_qualifying_results(bronze_dir: Path,
                              start_year: int,
                              end_year:   int,
                              force:      bool = False) -> pd.DataFrame:
    """
    Fetch qualifying results for all seasons and rounds.
    Saves one parquet per race and one combined master file.

    Parameters:
        bronze_dir : Path to bronze folder
        start_year : First season to ingest
        end_year   : Last season to ingest
        force      : Re-fetch all rounds even if files exist
    """
    bronze_dir   = Path(bronze_dir)
    quali_dir    = bronze_dir / "qualifying"
    quali_dir.mkdir(parents=True, exist_ok=True)

    master_path  = quali_dir / "all_seasons_qualifying.parquet"
    current_year = datetime.datetime.now().year
    new_dfs      = []

    for year in range(start_year, end_year + 1):
        is_current_season = (year == current_year)

        logging.info(f"\n── Season {year} ──────────────────────────────")

        rounds = get_season_rounds(year)
        if not rounds:
            logging.warning(f"  No rounds found for {year} — skipping")
            continue

        logging.info(f"  {len(rounds)} rounds found")

        for round_num in rounds:
            save_path = quali_dir / f"{year}_R{round_num:02d}_qualifying.parquet"

            # ── Skip if already fetched ───────────────────────────────────
            if save_path.exists() and save_path.stat().st_size > 0 \
                    and not force and not is_current_season:
                logging.info(f"  R{round_num:02d} already exists — skipping")
                continue

            # ── Fetch ─────────────────────────────────────────────────────
            logging.info(f"  Fetching {year} R{round_num:02d} qualifying...")
            df_round = fetch_round_qualifying(year, round_num)

            if df_round.empty:
                logging.warning(f"  No data for {year} R{round_num:02d} — skipping")
                continue

            # ── Save individual file ──────────────────────────────────────
            df_round.to_parquet(save_path, index=False, compression="snappy")
            logging.info(
                f"  Saved {year}_R{round_num:02d}_qualifying.parquet "
                f"({len(df_round)} rows)"
            )

            new_dfs.append(df_round)

        # Small pause between seasons to be polite to the API
        if year < end_year:
            logging.info(f"  Pausing {INTER_SEASON_DELAY}s between seasons...")
            time.sleep(INTER_SEASON_DELAY)

    # ── Update master file ────────────────────────────────────────────────
    logging.info("\n── Updating master file ─────────────────────")

    if new_dfs:
        new_data = pd.concat(new_dfs, ignore_index=True)

        if master_path.exists():
            master = pd.read_parquet(master_path)

            # Remove current season rows to avoid duplicates on re-fetch
            master = master[master["season"] != current_year]

            # Extend with new data
            master = pd.concat([master, new_data], ignore_index=True)
            master = master.sort_values(["season", "round_number", "quali_position"]) \
                           .reset_index(drop=True)
        else:
            # Master does not exist — build from all individual files
            logging.info("  Master not found — building from individual files...")
            all_files = sorted(quali_dir.glob("*_R*_qualifying.parquet"))

            if not all_files:
                logging.warning("  No qualifying parquet files found.")
                master = pd.DataFrame()
            else:
                master = pd.concat(
                    [pd.read_parquet(f) for f in all_files],
                    ignore_index=True
                ).sort_values(["season", "round_number", "quali_position"]) \
                 .reset_index(drop=True)

        master.to_parquet(master_path, index=False, compression="snappy")
        logging.info(f"  Master updated — {len(master)} total rows")

    else:
        logging.info("  No new rounds fetched — loading master from disk")
        master = pd.read_parquet(master_path)

    logging.info(f"\n{'='*50}")
    logging.info(f"DONE — {len(master)} total qualifying rows")
    logging.info(f"{'='*50}")

    return master


# =============================================================================
# CLI
# =============================================================================

if __name__ == "__main__":

    BRONZE_DIR = Path(r'C:\Users\Asus\Desktop\Formula1\data\bronze')

    df = fetch_qualifying_results(
        bronze_dir = BRONZE_DIR,
        start_year = config.START_YEAR,
        end_year   = config.END_YEAR,
        force      = config.FORCE,
    )

    print(f"\nShape         : {df.shape}")
    print(f"Seasons       : {sorted(df['season'].unique())}")
    print(f"Columns       : {list(df.columns)}")
    print(f"Pole sitters  : {df[df['quali_position'] == 1]['driver_ref'].value_counts().head()}")
    print(f"\nSample:\n{df.head()}")