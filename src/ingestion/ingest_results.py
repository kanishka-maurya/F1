import datetime
import requests
import time
import pandas as pd
from pathlib import Path
from src.utils import config
from src.utils.logger import logging 
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://api.jolpi.ca/ergast/f1"
RATE_LIMIT_DELAY = 1.0
INTER_SEASON_DELAY = 3.0



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
    'Retry-After' header we honour it instead of using our own schedule.
    """
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        respect_retry_after_header=True,
        raise_on_status=False,   # we call raise_for_status ourselves
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session
 
 
SESSION = _build_session()


# =============================================================================
# HELPERS
# =============================================================================
 

def _safe_get(url: str, timeout: int = 30) -> requests.Response:
    """
    GET with:
      • a fixed inter-request delay (RATE_LIMIT_DELAY)
      • the session's built-in retry / backoff
      • a manual fallback for 429s that slip through (reads Retry-After)
 
    Raises requests.HTTPError on non-2xx after all retries are exhausted.
    """
    time.sleep(RATE_LIMIT_DELAY)
 
    while True:
        response = SESSION.get(url, timeout=timeout)
 
        if response.status_code == 429:
            # The urllib3 retry layer should have caught this, but handle it
            # here as a belt-and-suspenders guard.
            wait = int(response.headers.get("Retry-After", 10))
            logging.warning(f"  429 received — waiting {wait}s before retry…")
            time.sleep(wait)
            continue  # retry immediately after the back-off window
 
        response.raise_for_status()
        return response

def get_season_rounds(year: int) -> list[int]:
    """
    Fetch list of round numbers for a given season from Jolpica.
    Returns empty list if season not found.
    """
    url = f"{BASE_URL}/{year}/races.json?limit=30"
    try:
        response = _safe_get(url)
        races = response.json()["MRData"]["RaceTable"]["Races"]
        return [int(r["round"]) for r in races]
    except Exception as e:
        logging.error(f"  Could not fetch rounds for {year}: {e}")
        return []
    

def extract_results(races: list, year: int, round_num: int) -> pd.DataFrame:
    """
    Extract and flatten race results from API response into a DataFrame.
    One row per driver per race.
    """
    if not races:
        return pd.DataFrame()
 
    race    = races[0]   # one round = one race
    records = []
 
    for r in race.get("Results", []):
        driver      = r.get("Driver",      {})
        constructor = r.get("Constructor", {})
        time_obj    = r.get("Time",        {})
        fastest_lap = r.get("FastestLap",  {})
        avg_speed   = fastest_lap.get("AverageSpeed", {})
 
        records.append({
            # Race context
            "season":               year,
            "round_number":         round_num,
            "race_name":            race.get("raceName"),
            "circuit_ref":          race.get("Circuit", {}).get("circuitId"),
            "race_date":            race.get("date"),
 
            # Driver
            "driver_ref":           driver.get("driverId"),
            "driver_code":          driver.get("code"),
            "driver_number":        r.get("number"),
 
            # Constructor
            "constructor_ref":      constructor.get("constructorId"),
 
            # Result
            "grid_position":        r.get("grid"),
            "finish_position":      r.get("position"),
            "position_text":        r.get("positionText"),   # R=retired, D=DSQ
            "points":               r.get("points"),
            "laps_completed":       r.get("laps"),
            "status":               r.get("status"),
            "race_time_millis":     time_obj.get("millis"),
            "race_time_text":       time_obj.get("time"),
 
            # Fastest lap
            "fastest_lap_number":   fastest_lap.get("lap"),
            "fastest_lap_time":     fastest_lap.get("Time", {}).get("time"),
            "fastest_lap_rank":     fastest_lap.get("rank"),
            "fastest_lap_speed":    avg_speed.get("speed"),
            "fastest_lap_speed_units": avg_speed.get("units"),
        })
 
    df = pd.DataFrame(records)
 
    # ── Type casting ──────────────────────────────────────────────────────
    int_cols = ["grid_position", "finish_position", "driver_number",
                "laps_completed", "fastest_lap_number", "fastest_lap_rank",
                "race_time_millis"]
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
 
    df["points"]    = pd.to_numeric(df["points"],    errors="coerce")
    df["race_date"] = pd.to_datetime(df["race_date"], errors="coerce").dt.date
 
    # ── Derived boolean flags ─────────────────────────────────────────────
    df["is_winner"]  = df["finish_position"] == 1
    df["is_podium"]  = df["finish_position"].isin([1, 2, 3])
    df["is_points"]  = df["points"] > 0
    df["is_dnf"]     = ~df["position_text"].isin(
                        [str(i) for i in range(1, 21)]
                       )
 
    return df
 
 
def fetch_round_results(year: int, round_num: int) -> pd.DataFrame:
    """Fetch results for a single race round."""
    url = f"{BASE_URL}/{year}/{round_num}/results.json"
    try:
        response = _safe_get(url)
        races = response.json()["MRData"]["RaceTable"]["Races"]
        return extract_results(races, year, round_num)
    except Exception as e:
        logging.error(f"  API call failed for {year} R{round_num:02d}: {e}")
        return pd.DataFrame()
    

# =============================================================================
# MAIN INGESTION FUNCTION
# =============================================================================
 

def fetch_race_results(bronze_dir: Path,
                       start_year: int,
                       end_year:   int,
                       force:      bool = False) -> pd.DataFrame:
    """
    Fetch race results for all seasons and rounds.
    Saves one parquet per race and one combined master file.
 
    Parameters:
        bronze_dir : Path to bronze folder
        start_year : First season to ingest
        end_year   : Last season to ingest
        force      : Re-fetch all races even if files exist
    """
    bronze_dir   = Path(bronze_dir)
    results_dir  = bronze_dir / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
 
    master_path  = results_dir / "all_seasons_results.parquet"
    current_year = datetime.datetime.now().year
    new_dfs      = []   # only newly fetched races
 
    for year in range(start_year, end_year + 1):
        is_current_season = (year == current_year)
 
        logging.info(f"\n── Season {year} ──────────────────────────────")
 
        # Fetch round list for this season
        rounds = get_season_rounds(year)
        if not rounds:
            logging.warning(f"  No rounds found for {year} — skipping")
            continue
 
        logging.info(f"  {len(rounds)} rounds found")
 
        for round_num in rounds:
            save_path = results_dir / f"{year}_R{round_num:02d}_results.parquet"
 
            # ── Skip if already fetched ───────────────────────────────────
            if save_path.exists() and save_path.stat().st_size > 0 and not force and not is_current_season:
                logging.info(f"  R{round_num:02d} already exists — skipping")
                continue
 
            # ── Fetch this round ──────────────────────────────────────────
            logging.info(f"  Fetching {year} R{round_num:02d}...")
            df_round = fetch_round_results(year, round_num)
 
            if df_round.empty:
                logging.warning(f"  No data for {year} R{round_num:02d} — skipping")
                continue
 
            # ── Save individual race file ─────────────────────────────────
            df_round.to_parquet(save_path, index=False, compression="snappy")
            logging.info(f"  Saved {year}_R{round_num:02d}_results.parquet ({len(df_round)} rows)")
 
            new_dfs.append(df_round)
 
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
            master = master.sort_values(["season", "round_number", "finish_position"]) \
                           .reset_index(drop=True)
        else:
            # Master does not exist — build from all individual files
            logging.info("  Master not found — building from individual files...")
            all_files = sorted(results_dir.glob("*_R*_results.parquet"))

            if not all_files:
                logging.warning("No race parquet files found.")
                master = pd.DataFrame()
            
            else: 
                master    = pd.concat(
                    [pd.read_parquet(f) for f in all_files],
                    ignore_index=True
                ).sort_values(["season", "round_number", "finish_position"]) \
                .reset_index(drop=True)
 
        master.to_parquet(master_path, index=False, compression="snappy")
        logging.info(f"  Master updated — {len(master)} total rows")
 
    else:
        logging.info("  No new races fetched — loading master from disk")
        master = pd.read_parquet(master_path)
 
    logging.info(f"\n{'='*50}")
    logging.info(f"DONE — {len(master)} total result rows")
    logging.info(f"{'='*50}")
 
    return master

# ── Run ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":

    BRONZE_DIR = Path(r'C:\Users\Asus\Desktop\Formula1\data\bronze')
 
    df = fetch_race_results(
        bronze_dir = BRONZE_DIR,
        start_year = config.START_YEAR,
        end_year   = config.END_YEAR,
        force      = config.FORCE,
    )
 
    print(f"\nShape       : {df.shape}")
    print(f"Seasons     : {sorted(df['season'].unique())}")
    print(f"Winners     : {df[df['is_winner']]['driver_ref'].value_counts().head()}")
    print(f"\nSample:\n{df.head()}")