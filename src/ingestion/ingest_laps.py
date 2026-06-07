"""
Lap Data Ingestion — Jolpica API → Bronze Parquet
=================================================
Endpoint : https://api.jolpi.ca/ergast/f1/{year}/{round}/laps.json
Coverage : 2010 – 2025, all races, all drivers, all laps

Response structure (nested):
    Race → Laps[lap_number] → Timings[driver] → position, time

So one API response looks like:
    Lap 1 → [verstappen pos=1 time=1:38.1, leclerc pos=2 time=1:38.5, ...]
    Lap 2 → [verstappen pos=1 time=1:32.4, leclerc pos=2 time=1:32.8, ...]

We flatten this to one row per driver per lap.

Output:
    bronze/laps/
    ├── 2010_R01_laps.parquet      (~1,200 rows)
    ├── 2010_R02_laps.parquet
    ├── ...
    ├── 2025_R24_laps.parquet
    └── all_seasons_laps.parquet   ← combined master (~432,000 rows)

Pagination:
    Each race has ~1,200 rows. With limit=100 that is ~12 API calls per race.
    With ~340 races total that is ~4,000 API calls for full ingestion.
    RATE_LIMIT_DELAY ensures we don't hammer the API.

Usage:
    python -m src.ingestion.ingest_laps
    python -m src.ingestion.ingest_laps --force
    python -m src.ingestion.ingest_laps --year 2024          # single season
    python -m src.ingestion.ingest_laps --year 2024 --round 1 # single race
"""

import datetime
import logging
import time
import requests
import pandas as pd
from pathlib import Path
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from src.utils import config
from src.utils.logger import logging


BASE_URL           = "https://api.jolpi.ca/ergast/f1"
RATE_LIMIT_DELAY   = 2.0   # seconds between every API call
INTER_SEASON_DELAY = 3.0   # extra pause between seasons
PAGE_LIMIT         = 100   # rows per API page


# =============================================================================
# SESSION WITH RETRY + BACKOFF
# =============================================================================

def _build_session() -> requests.Session:
    """
    requests.Session with automatic retry and exponential backoff.

    Retry schedule (backoff_factor=2):
        attempt 1 → wait  2s
        attempt 2 → wait  4s
        attempt 3 → wait  8s
        attempt 4 → wait 16s
        attempt 5 → wait 32s
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
    GET with rate limit delay, retry/backoff, and manual 429 handling.
    """
    time.sleep(RATE_LIMIT_DELAY)

    while True:
        response = SESSION.get(url, timeout=timeout)

        if response.status_code == 429:
            wait = max(int(response.headers.get("Retry-After", 30)), 30)
            logging.warning(f"  429 received — waiting {wait}s...")
            time.sleep(wait)
            continue

        response.raise_for_status()
        return response


# =============================================================================
# HELPERS
# =============================================================================

def get_season_rounds(year: int) -> list[int]:
    """
    Fetch all round numbers for a season.
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
    Convert lap time string to milliseconds.

    Examples:
        "1:32.456" → 92456.0
        "92.456"   → 92456.0
        None       → None
    """
    if not laptime_str or pd.isnull(laptime_str):
        return None
    try:
        s = str(laptime_str).strip()
        if ":" in s:
            minutes, seconds = s.split(":")
            total_seconds    = int(minutes) * 60 + float(seconds)
        else:
            total_seconds = float(s)
        return round(total_seconds * 1000, 1)
    except Exception:
        return None


# =============================================================================
# FETCH ALL PAGES FOR ONE RACE
# =============================================================================

def fetch_all_lap_pages(year: int, round_num: int) -> list[dict]:
    """
    Fetch all paginated lap data for a single race.

    The API returns laps grouped by lap number, each with a Timings list.
    We fetch all pages and return the raw combined Laps list.

    Returns:
        List of lap dicts:
        [
            {"number": "1", "Timings": [{"driverId": ..., "position": ..., "time": ...}]},
            {"number": "2", "Timings": [...]},
            ...
        ]
    """
    all_laps = []
    offset   = 0

    while True:
        url = (f"{BASE_URL}/{year}/{round_num}/laps.json"
               f"?limit={PAGE_LIMIT}&offset={offset}")
        try:
            response = _safe_get(url)
            data     = response.json()["MRData"]
            total    = int(data["total"])
            races    = data["RaceTable"]["Races"]

            if not races:
                # Race exists in schedule but lap data not available yet
                logging.warning(f"  No lap data for {year} R{round_num:02d}")
                return []

            laps = races[0].get("Laps", [])
            all_laps.extend(laps)

            logging.info(
                f"  Page offset={offset}: "
                f"fetched {len(all_laps)}/{total} lap entries"
            )

            offset += PAGE_LIMIT
            if offset >= total:
                break

        except Exception as e:
            logging.error(
                f"  Page fetch failed {year} R{round_num:02d} "
                f"offset={offset}: {e}"
            )
            raise

    return all_laps


# =============================================================================
# EXTRACT — flatten nested lap structure
# =============================================================================

def extract_laps(all_laps: list[dict], year: int,
                 round_num: int, race_info: dict) -> pd.DataFrame:
    """
    Flatten the nested lap structure into one row per driver per lap.

    Input structure:
        [
          {
            "number": "1",
            "Timings": [
              {"driverId": "verstappen", "position": "1", "time": "1:38.149"},
              {"driverId": "leclerc",    "position": "2", "time": "1:38.576"},
            ]
          },
          ...
        ]

    Output (one row per driver per lap):
        season  round  driver_ref   lap_number  position  lap_time  lap_time_ms
        2024    1      verstappen   1           1         1:38.149  98149.0
        2024    1      leclerc      1           2         1:38.576  98576.0
        2024    1      verstappen   2           1         1:32.401  92401.0
        ...
    """
    if not all_laps:
        return pd.DataFrame()

    records = []

    for lap in all_laps:
        lap_number = int(lap.get("number", 0))
        timings    = lap.get("Timings", [])

        for t in timings:
            records.append({
                "season":       year,
                "round_number": round_num,
                "race_name":    race_info.get("race_name"),
                "circuit_ref":  race_info.get("circuit_ref"),
                "race_date":    race_info.get("race_date"),
                "driver_ref":   t.get("driverId"),
                "lap_number":   lap_number,
                "position":     t.get("position"),
                "lap_time":     t.get("time"),
                "lap_time_ms":  parse_laptime_to_ms(t.get("time")),
            })

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)

    # ── Type casting ──────────────────────────────────────────────────────
    df["lap_number"] = pd.to_numeric(df["lap_number"], errors="coerce").astype("Int16")
    df["position"]   = pd.to_numeric(df["position"],   errors="coerce").astype("Int16")
    df["race_date"]  = pd.to_datetime(df["race_date"],  errors="coerce").dt.date


    # Sort final output
    df = df.sort_values(["lap_number", "position"]).reset_index(drop=True)

    return df


def get_race_info(year: int, round_num: int) -> dict:
    """
    Fetch basic race metadata (name, circuit, date) for context columns.
    """
    url = f"{BASE_URL}/{year}/{round_num}/races.json"
    try:
        response = _safe_get(url)
        races    = response.json()["MRData"]["RaceTable"]["Races"]
        if not races:
            return {}
        race = races[0]
        return {
            "race_name":   race.get("raceName"),
            "circuit_ref": race.get("Circuit", {}).get("circuitId"),
            "race_date":   race.get("date"),
        }
    except Exception as e:
        logging.warning(f"  Could not fetch race info for {year} R{round_num}: {e}")
        return {}


# =============================================================================
# SINGLE RACE INGESTION
# =============================================================================

def fetch_race_laps(year: int, round_num: int) -> pd.DataFrame:
    """
    Fetch, flatten, and return all lap data for one race.
    """
    race_info = get_race_info(year, round_num)
    all_laps  = fetch_all_lap_pages(year, round_num)

    if not all_laps:
        return pd.DataFrame()

    df = extract_laps(all_laps, year, round_num, race_info)
    logging.info(
        f"  Extracted {len(df)} rows for "
        f"{year} R{round_num:02d} ({race_info.get('race_name', '?')})"
    )
    return df


# =============================================================================
# MAIN INGESTION FUNCTION
# =============================================================================

def fetch_lap_data(bronze_dir:  Path,
                   start_year:  int,
                   end_year:    int,
                   force:       bool = False,
                   single_year: int  = None,
                   single_round: int = None) -> pd.DataFrame:
    """
    Fetch lap data for all seasons and rounds.
    Saves one parquet per race and one combined master file.

    Parameters:
        bronze_dir   : Path to bronze folder
        start_year   : First season to ingest
        end_year     : Last season to ingest
        force        : Re-fetch all races even if files exist
        single_year  : Ingest only this season (overrides start/end)
        single_round : Ingest only this round within single_year
    """
    bronze_dir   = Path(bronze_dir)
    laps_dir     = bronze_dir / "laps"
    laps_dir.mkdir(parents=True, exist_ok=True)

    master_path  = laps_dir / "all_seasons_laps.parquet"
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
            save_path = laps_dir / f"{year}_R{round_num:02d}_laps.parquet"

            # ── Skip if already fetched ───────────────────────────────────
            if save_path.exists() and save_path.stat().st_size > 0 \
                    and not force and not is_current_season:
                logging.info(f"  R{round_num:02d} already exists — skipping")
                continue

            # ── Fetch ─────────────────────────────────────────────────────
            logging.info(f"  Fetching {year} R{round_num:02d} laps...")
            try:
                df_round = fetch_race_laps(year, round_num)
            except Exception as e:
                logging.error(
                    f"  Failed {year} R{round_num:02d}: {e} — skipping"
                )
                continue

            if df_round.empty:
                logging.warning(
                    f"  No lap data for {year} R{round_num:02d} — skipping"
                )
                continue

            # ── Save individual race file ─────────────────────────────────
            df_round.to_parquet(save_path, index=False, compression="snappy")
            logging.info(
                f"  Saved {year}_R{round_num:02d}_laps.parquet "
                f"({len(df_round):,} rows)"
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

            # Remove current season to avoid duplicates
            master = master[master["season"] != current_year]

            master = pd.concat([master, new_data], ignore_index=True)
            master = master.sort_values(
                ["season", "round_number", "lap_number", "position"]
            ).reset_index(drop=True)

        else:
            # Build master from all individual files
            logging.info("  Master not found — building from individual files...")
            all_files = sorted(laps_dir.glob("*_R*_laps.parquet"))

            if not all_files:
                logging.warning("  No lap parquet files found.")
                master = pd.DataFrame()
            else:
                master = pd.concat(
                    [pd.read_parquet(f) for f in all_files],
                    ignore_index=True
                ).sort_values(
                    ["season", "round_number", "lap_number", "position"]
                ).reset_index(drop=True)

        master.to_parquet(master_path, index=False, compression="snappy")
        logging.info(f"  Master updated — {len(master):,} total rows")

    else:
        logging.info("  No new races fetched — loading master from disk")
        master = pd.read_parquet(master_path)

    logging.info(f"\n{'='*50}")
    logging.info(f"DONE — {len(master):,} total lap rows")
    logging.info(f"{'='*50}")

    return master



# ── Run ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    BRONZE_DIR = Path(r'C:\Users\Asus\Desktop\Formula1\data\bronze')

    df = fetch_lap_data(
        bronze_dir   = BRONZE_DIR,
        start_year   = config.START_YEAR,
        end_year     = config.END_YEAR,
        force        = config.FORCE,
        single_year  = config.YEAR,
        single_round = config.ROUND,
    )

    print(f"\nShape    : {df.shape}")
    print(f"Seasons  : {sorted(df['season'].unique())}")
    print(f"Columns  : {list(df.columns)}")
    print(f"\nSample:\n{df.head(10)}")