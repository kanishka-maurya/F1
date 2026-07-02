import datetime
import requests
import time
import pandas as pd
from pathlib import Path
from src.utils import config
from src.utils.logger import logging 
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from src.utils.utility import safe_get, build_session, get_season_rounds, parse_laptime_to_ms


# =============================================================================
# EXTRACT
# =============================================================================

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
            "race_time_millis":     time_obj.get("millis")
        })
 
    df = pd.DataFrame(records)
 
    # ── Type casting ──────────────────────────────────────────────────────
    int_cols = ["grid_position", "finish_position", "driver_number",
                "laps_completed", "race_time_millis"]
    
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
 
    df["points"]    = pd.to_numeric(df["points"],    errors="coerce")
    df["race_date"] = pd.to_datetime(df["race_date"], errors="coerce").dt.date
 
    return df
 
 
def fetch_round_results(session: requests.Session, timeout: int, year: int, round_num: int) -> pd.DataFrame:
    """Fetch results for a single race round."""
    url = f"{config.BASE_URL}/{year}/{round_num}/results.json"
    try:
        response = safe_get(session=session, url=url, timeout=timeout)
        races = response.json()["MRData"]["RaceTable"]["Races"]
        return extract_results(races, year, round_num)
    except Exception as e:
        logging.error(f"  API call failed for {year} R{round_num:02d}: {e}")
        return pd.DataFrame()
    

# =============================================================================
# MAIN INGESTION FUNCTION
# =============================================================================

def fetch_race_results(
                       session: requests.Session, 
                       schedule_dir: Path,
                       bronze_dir: Path,
                       start_year: int,
                       end_year:   int,
                       timeout: int, 
                       force:      bool) -> pd.DataFrame:
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
        rounds = get_season_rounds(schedule_dir=schedule_dir, year=year)
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
            df_round = fetch_round_results(session=session, year=year, round_num=round_num, timeout=timeout)
 
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
        # ── No new data ───────────────────────────────────────────────────────
        if not master_path.exists():
            logging.warning("  No new data and no master file — returning empty | Set FORCE = True.")
            return pd.DataFrame()

        logging.info("  No new rounds fetched — master unchanged, loading from disk")
        master = pd.read_parquet(master_path)
 
    return master

# ── Run ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # BUILD SESSION
    SESSION = build_session()

 
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
    
    print("\nRACE RESULTS DATA:")
    logging.info(df.head())
    print(df.head())