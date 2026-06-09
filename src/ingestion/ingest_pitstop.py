import datetime
import time
import requests
import pandas as pd
from pathlib import Path
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from src.utils import config
from src.utils.logger import logging
from src.utils.utility import safe_get, build_session, get_season_rounds, parse_laptime_to_ms

# BUILD SESSION
SESSION = build_session()


def get_season_rounds_from_api(year: int) -> list[int]:
    """Fetch round numbers from Jolpica API (fallback only)."""
    url = f"{config.BASE_URL}/{year}/races.json?limit=30"
    try:
        response = safe_get(url)
        races    = response.json()["MRData"]["RaceTable"]["Races"]
        return [int(r["round"]) for r in races]
    except Exception as e:
        logging.error(f"  Could not fetch rounds for {year}: {e}")
        return []


# =============================================================================
# FETCH ALL PAGES FOR ONE RACE
# =============================================================================

def fetch_all_pitstop_pages(year: int, round_num: int) -> list[dict]:
    """
    Fetch all paginated pit stop data for a single race.

    Pit stop data is small — most races have 30–50 pit stops total
    so usually only one page needed. Pagination handled anyway for safety.

    Returns:
        List of raw pit stop dicts from the API
    """
    all_pitstops = []
    offset       = 0

    while True:
        url = (f"{config.BASE_URL}/{year}/{round_num}/pitstops.json"
               f"?limit={config.PAGE_LIMIT}&offset={offset}")
        try:
            response = safe_get(url)
            data     = response.json()["MRData"]
            total    = int(data["total"])
            races    = data["RaceTable"]["Races"]

            if not races:
                logging.warning(
                    f"  No pit stop data for {year} R{round_num:02d}"
                )
                return []

            pitstops = races[0].get("PitStops", [])
            all_pitstops.extend(pitstops)

            logging.info(
                f"  Page offset={offset}: "
                f"fetched {len(all_pitstops)}/{total} pit stops"
            )

            offset += config.PAGE_LIMIT
            if offset >= total:
                break

        except Exception as e:
            logging.error(
                f"  Page fetch failed {year} R{round_num:02d} "
                f"offset={offset}: {e}"
            )
            raise

    return all_pitstops


# =============================================================================
# EXTRACT
# =============================================================================

def extract_pitstops(all_pitstops: list[dict], year: int,
                     round_num: int, race_info: dict) -> pd.DataFrame:
    """
    Extract and flatten pit stop data into one row per pit stop.

    API response structure:
        {
            "driverId":  "verstappen",
            "lap":       "14",
            "stop":      "1",
            "time":      "13:42:31",    ← local clock time of pit stop
            "duration":  "23.456"       ← seconds in pit lane
        }

    Derived columns:
        duration_ms      : duration converted to milliseconds
    """
    if not all_pitstops:
        return pd.DataFrame()

    records = []
    for p in all_pitstops:
        duration_str = p.get("duration")
        duration_ms  = parse_laptime_to_ms(duration_str)

        records.append({
            # Race context
            "season":          year,
            "round_number":    round_num,
            "race_name":       race_info.get("race_name"),
            "circuit_ref":     race_info.get("circuit_ref"),
            "race_date":       race_info.get("race_date"),

            # Pit stop data
            "driver_ref":      p.get("driverId"),
            "stop_number":     p.get("stop"),
            "lap_number":      p.get("lap"),
            "local_time":      p.get("time"),
            "duration":        duration_str,
            "duration_ms":     duration_ms,
        })

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)

    # ── Type casting ──────────────────────────────────────────────────────
    df["stop_number"] = pd.to_numeric(df["stop_number"], errors="coerce").astype("Int16")
    df["lap_number"]  = pd.to_numeric(df["lap_number"],  errors="coerce").astype("Int16")
    df["duration_ms"] = pd.to_numeric(df["duration_ms"], errors="coerce")
    df["race_date"]   = pd.to_datetime(df["race_date"],  errors="coerce").dt.date


    # ── Race-level pit stop stats per driver ──────────────────────────────
    # Total stops per driver in this race
    stop_counts = (
        df.groupby("driver_ref")["stop_number"]
        .max()
        .reset_index()
        .rename(columns={"stop_number": "total_stops_race"})
    )
    df = df.merge(stop_counts, on="driver_ref", how="left")

    # Average pit duration per driver this race
    avg_duration = (
        df.groupby("driver_ref")["duration_ms"]
        .mean()
        .round(1)
        .reset_index()
        .rename(columns={"duration_ms": "avg_duration_ms_race"})
    )
    df = df.merge(avg_duration, on="driver_ref", how="left")

    # ── Sort ──────────────────────────────────────────────────────────────
    df = df.sort_values(["driver_ref", "stop_number"]).reset_index(drop=True)

    return df


def get_race_info(year: int, round_num: int) -> dict:
    """Fetch basic race metadata for context columns."""
    schedule_path = None  # will use API fallback
    url = f"{config.BASE_URL}/{year}/{round_num}/races.json"
    try:
        response = safe_get(url)
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
        logging.warning(f"  Could not fetch race info {year} R{round_num}: {e}")
        return {}


def get_race_info_from_disk(schedule_dir: Path,
                            year: int, round_num: int) -> dict:
    """
    Read race metadata from saved schedule parquet — zero API calls.
    Falls back to API if schedule not found.
    """
    schedule_path = schedule_dir / f"{year}_schedule.parquet"

    if schedule_path.exists():
        df   = pd.read_parquet(schedule_path)
        race = df[df["round_number"] == round_num]
        if not race.empty:
            row = race.iloc[0]
            return {
                "race_name":   row.get("race_name"),
                "circuit_ref": row.get("circuit_ref"),
                "race_date":   str(row.get("race_date", "")),
            }

    # Fallback to API
    logging.warning(
        f"  Schedule not on disk for {year} R{round_num} — fetching race info from API"
    )
    return get_race_info(year, round_num)


# =============================================================================
# SINGLE RACE PIT STOP FETCH
# =============================================================================

def fetch_race_pitstops(year: int, round_num: int,
                        schedule_dir: Path) -> pd.DataFrame:
    """
    Fetch and extract all pit stop data for one race.
    """
    race_info    = get_race_info_from_disk(schedule_dir, year, round_num)
    all_pitstops = fetch_all_pitstop_pages(year, round_num)

    if not all_pitstops:
        return pd.DataFrame()

    df = extract_pitstops(all_pitstops, year, round_num, race_info)
    logging.info(
        f"  Extracted {len(df)} pit stops for "
        f"{year} R{round_num:02d} ({race_info.get('race_name', '?')})"
    )
    return df


# =============================================================================
# MAIN INGESTION FUNCTION
# =============================================================================

def fetch_pit_stops(
                    schedule_dir: Path,
                    bronze_dir:   Path,
                    start_year:   int,
                    end_year:     int,
                    force:        bool = False,
                    single_year:  int  = None,
                    single_round: int  = None) -> pd.DataFrame:
    """
    Fetch pit stop data for all seasons and rounds.
    Saves one parquet per race and one combined master file.

    Parameters:
        bronze_dir   : Path to bronze folder
        start_year   : First season to ingest (min 2012)
        end_year     : Last season to ingest
        force        : Re-fetch all races even if files exist
        single_year  : Ingest only this season
        single_round : Ingest only this round within single_year
    """
    bronze_dir   = Path(bronze_dir)
    pitstops_dir = bronze_dir / "pit_stops"
    pitstops_dir.mkdir(parents=True, exist_ok=True)

    master_path  = pitstops_dir / "all_seasons_pit_stops.parquet"
    current_year = datetime.datetime.now().year
    new_dfs      = []

    # ── Determine year range ──────────────────────────────────────────────
    if single_year:
        year_range = [single_year]
    else:
        # Clamp start year to 2012 minimum
        effective_start = max(start_year, config.PIT_DATA_START_YEAR)
        if start_year < config.PIT_DATA_START_YEAR:
            logging.warning(
                f"  Pit stop data not available before {config.PIT_DATA_START_YEAR}. "
                f"Starting from {config.PIT_DATA_START_YEAR}."
            )
        year_range = range(effective_start, end_year + 1)

    for year in year_range:
        is_current_season = (year == current_year)

        logging.info(f"\n── Season {year} ──────────────────────────────")

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
            save_path = pitstops_dir / f"{year}_R{round_num:02d}_pit_stops.parquet"

            # ── Skip if already fetched ───────────────────────────────────
            if save_path.exists() and save_path.stat().st_size > 0 \
                    and not force and not is_current_season:
                logging.info(f"  R{round_num:02d} already exists — skipping")
                continue

            # ── Fetch ─────────────────────────────────────────────────────
            logging.info(f"  Fetching {year} R{round_num:02d} pit stops...")
            try:
                df_round = fetch_race_pitstops(year, round_num, schedule_dir)
            except Exception as e:
                logging.error(
                    f"  Failed {year} R{round_num:02d}: {e} — skipping"
                )
                continue

            if df_round.empty:
                logging.warning(
                    f"  No pit stop data for {year} R{round_num:02d} — skipping"
                )
                continue

            # ── Save individual race file ─────────────────────────────────
            df_round.to_parquet(save_path, index=False, compression="snappy")
            logging.info(
                f"  Saved {year}_R{round_num:02d}_pit_stops.parquet "
                f"({len(df_round)} stops)"
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
                ["season", "round_number", "driver_ref", "stop_number"]
            ).reset_index(drop=True)

        else:
            # Build master from all individual files
            logging.info("  Master not found — building from individual files...")
            all_files = sorted(pitstops_dir.glob("*_R*_pit_stops.parquet"))

            if not all_files:
                logging.warning("  No pit stop parquet files found.")
                master = pd.DataFrame()
            else:
                master = pd.concat(
                    [pd.read_parquet(f) for f in all_files],
                    ignore_index=True
                ).sort_values(
                    ["season", "round_number", "driver_ref", "stop_number"]
                ).reset_index(drop=True)

        master.to_parquet(master_path, index=False, compression="snappy")
        logging.info(f"  Master updated — {len(master):,} total rows")

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


    df = fetch_pit_stops(
        schedule_dir = config.BRONZE_DIR / "schedule", 
        bronze_dir   = config.BRONZE_DIR,
        start_year   = config.START_YEAR,
        end_year     = config.END_YEAR,
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