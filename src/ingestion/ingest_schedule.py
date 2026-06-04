import pandas as pd
import requests
from pathlib import Path
from src.utils.logger import logging
from src.utils import config
import datetime 
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time


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
        if save_path.exists() and save_path.stat().st_size > 0 and not force and not is_current_season:
            logging.info(f"  {year} already exists — skipping")
            continue

        # ── Fetch from API ────────────────────────────────────────────────
        logging.info(f"  Fetching {year} schedule...")
        try:
            url      = f"https://api.jolpi.ca/ergast/f1/{year}/races.json?limit=30"
            response = _safe_get(url)
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

            if not all_files:
                master = pd.DataFrame()
            else:
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