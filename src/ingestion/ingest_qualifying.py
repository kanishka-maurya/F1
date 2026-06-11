import datetime
import time
import requests
import pandas as pd
from src.utils.logger import logging
from pathlib import Path
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from src.utils import config
from src.utils.utility import safe_get, build_session, get_season_rounds, parse_laptime_to_ms


# =============================================================================
# EXTRACT
# =============================================================================

def extract_qualifying(races: list, year: int, round_num: int) -> pd.DataFrame:
    """
    Extract and flatten qualifying results from API response.
    One row per driver per qualifying session.
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
            "q3_time":           q3_str
        })

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)

    # ── Type casting ──────────────────────────────────────────────────────
    df["quali_position"] = pd.to_numeric(df["quali_position"], errors="coerce")
    df["driver_number"]  = pd.to_numeric(df["driver_number"],  errors="coerce")
    df["race_date"]      = pd.to_datetime(df["race_date"], errors="coerce").dt.date

    return df


def fetch_round_qualifying(session: requests.Session, timeout: int, year: int, round_num: int) -> pd.DataFrame:

    """Fetch qualifying results for a single race round."""

    url = f"{config.BASE_URL}/{year}/{round_num}/qualifying.json"
    try:
        response = safe_get(session=session, url=url, timeout=timeout)
        races    = response.json()["MRData"]["RaceTable"]["Races"]
        return extract_qualifying(races, year, round_num)
    
    except Exception as e:
        logging.error(f"  API call failed for {year} R{round_num:02d}: {e}")
        return pd.DataFrame()


# =============================================================================
# MAIN INGESTION FUNCTION
# =============================================================================

def fetch_qualifying_results(
                            session: requests.Session,
                            schedule_dir: Path,
                            bronze_dir: Path,
                            start_year: int,
                            end_year:   int,
                            timeout: int,
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

        rounds = get_season_rounds(schedule_dir=schedule_dir, year=year)
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
            df_round = fetch_round_qualifying(session=session, year=year, round_num=round_num, timeout=timeout)

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
            logging.info(f"  Pausing {config.INTER_SEASON_DELAY}s between seasons...")
            time.sleep(config.INTER_SEASON_DELAY)

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
                return pd.DataFrame()
            else:
                master = pd.concat(
                    [pd.read_parquet(f) for f in all_files],
                    ignore_index=True
                ).sort_values(["season", "round_number", "quali_position"]) \
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

