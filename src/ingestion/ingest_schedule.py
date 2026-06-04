import pandas as pd
import requests
from pathlib import Path
from src.utils.logger import logging
from src.utils import config
import datetime 


def fetch_race_schedule(bronze_dir: Path,
                        start_year: int,
                        end_year: int,
                        force: bool = False) -> pd.DataFrame:

    bronze_dir   = Path(bronze_dir)
    schedule_dir = bronze_dir / "schedule"
    schedule_dir.mkdir(parents=True, exist_ok=True)

    master_path  = schedule_dir / "all_seasons_schedule.parquet"
    current_year = datetime.datetime.now().year

    new_season_dfs = []   # only newly fetched seasons

    for year in range(start_year, end_year + 1):
        save_path        = schedule_dir / f"{year}_schedule.parquet"
        is_current_season = (year == current_year)

        # ── Skip if file exists (re-fetch current season always) ──────────
        if save_path.exists() and not force and not is_current_season:
            logging.info(f"  {year} already exists — skipping")
            continue

        # ── Fetch from API ────────────────────────────────────────────────
        logging.info(f"  Fetching {year} schedule...")
        try:
            url      = f"https://api.jolpi.ca/ergast/f1/{year}/races.json?limit=30"
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            races    = response.json()["MRData"]["RaceTable"]["Races"]
        except Exception as e:
            logging.error(f"  Failed to fetch {year}: {e}")
            continue

        if not races:
            logging.warning(f"  No races found for {year} — skipping")
            continue

        # ── Extract ───────────────────────────────────────────────────────
        records = []
        for r in races:
            records.append({
                "season":       year,
                "round_number": int(r.get("round")),
                "race_name":    r.get("raceName"),
                "circuit_ref":  r["Circuit"].get("circuitId"),
                "race_date":    r.get("date"),
                "race_time":    r.get("time"),
                "fp1_date":     r.get("FirstPractice",  {}).get("date"),
                "fp2_date":     r.get("SecondPractice", {}).get("date"),
                "fp3_date":     r.get("ThirdPractice",  {}).get("date"),
                "quali_date":   r.get("Qualifying",     {}).get("date"),
                "sprint_date":  r.get("Sprint",         {}).get("date"),
                "has_sprint":   "Sprint" in r,
            })

        df_year = pd.DataFrame(records)

        # ── Clean dates ───────────────────────────────────────────────────
        date_cols = ["race_date", "fp1_date", "fp2_date",
                     "fp3_date", "quali_date", "sprint_date"]
        for col in date_cols:
            df_year[col] = pd.to_datetime(df_year[col], errors="coerce").dt.date

        # ── Save individual season file ───────────────────────────────────
        df_year.to_parquet(save_path, index=False, compression="snappy")
        logging.info(f"  Saved {year}_schedule.parquet ({len(df_year)} races)")

        new_season_dfs.append(df_year)

    # ── Update master file ────────────────────────────────────────────────
    if new_season_dfs:
        new_data = pd.concat(new_season_dfs, ignore_index=True)

        if master_path.exists():
            # Load existing master and extend with new seasons
            master   = pd.read_parquet(master_path)
            
            # Remove current season rows if they exist (avoid duplicates)
            master   = master[master["season"] != current_year]
            
            # Extend with new data
            master   = pd.concat([master, new_data], ignore_index=True)
            master   = master.sort_values(["season", "round_number"])\
                             .reset_index(drop=True)
        else:
            # Master does not exist yet — build from scratch
            # Load all individual files to build complete master
            all_files = sorted(schedule_dir.glob("*_schedule.parquet"))
            all_files = [f for f in all_files 
                         if f.name != "all_seasons_schedule.parquet"]
            master    = pd.concat(
                [pd.read_parquet(f) for f in all_files],
                ignore_index=True
            ).sort_values(["season", "round_number"]).reset_index(drop=True)

        master.to_parquet(master_path, index=False, compression="snappy")
        logging.info(f"  Master file updated — {len(master)} total races")

    else:
        # No new seasons — just load master as is
        logging.info("  No new seasons fetched — loading master from disk")
        master = pd.read_parquet(master_path)

    return master


# ── Run ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    BRONZE_DIR = Path(r'C:\Users\Asus\Desktop\Formula1\data\bronze')
 
    results = fetch_race_schedule(bronze_dir=BRONZE_DIR, start_year=config.START_YEAR, end_year=config.END_YEAR, force=config.FORCE)
 
    print("\nRACE SCHEDULE DATA:")
    logging.info(results.head())
    print(results.head())