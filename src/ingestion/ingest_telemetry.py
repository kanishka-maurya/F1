import datetime
import time
import requests
import warnings
import fastf1
import pandas as pd
import numpy as np
from pathlib import Path
from src.utils import config
from src.utils.logger import logging
from src.utils.utility import safe_get, build_session, get_season_rounds, parse_laptime_to_ms, get_race_info_from_disk


# HELPERS
def safe_mean(series) -> float | None:
    """Mean ignoring NaN. Returns None if all values are NaN."""
    try:
        val = series.mean()
        return round(float(val), 3) if not np.isnan(val) else None
    except Exception:
        return None


def safe_max(series) -> float | None:
    """Max ignoring NaN."""
    try:
        val = series.max()
        return round(float(val), 3) if not np.isnan(val) else None
    except Exception:
        return None


def safe_min(series) -> float | None:
    """Min ignoring NaN."""
    try:
        val = series.min()
        return round(float(val), 3) if not np.isnan(val) else None
    except Exception:
        return None


def safe_pct(condition_series) -> float | None:
    """
    Percentage of True values in a boolean series.
    e.g. CONDITION SERIES: throttle >= 99 → what % of the lap was at full throttle
    """
    try:
        if len(condition_series) == 0:
            return None
        return round(float(condition_series.mean() * 100), 2)
    except Exception:
        return None


def safe_int(val) -> int | None:
    try:
        if pd.isnull(val):
            return None
        return int(val)
    except Exception:
        return None

# =============================================================================
# AGGREGATE ONE LAP'S TELEMETRY
# =============================================================================

def aggregate_lap_telemetry(car_data: pd.DataFrame,
                            driver_code: str,
                            lap_number: int) -> dict | None:
    """
    Aggregate raw car telemetry for one lap into a single summary row.

    FastF1 car_data columns:
        Speed    : km/h (float)
        Throttle : 0–100 (float)
        Brake    : True/False (bool) or 0/1
        DRS      : 0–14 integer (0=closed, 10/12/14=open)
        nGear    : gear number 0–8
        RPM      : engine RPM (int)
        Time     : timedelta into session

    Parameters:
        car_data    : raw telemetry DataFrame for one lap
        driver_code : driver abbreviation e.g. 'VER'
        lap_number  : lap number int

    Returns:
        dict of aggregated metrics or None if data is unusable
    """
    if car_data is None or car_data.empty:
        return None

    # Need at least 10 samples to be meaningful
    # (some in/out laps have very few samples)
    if len(car_data) < 10:
        return None

    try:
        # ── Speed ─────────────────────────────────────────────────────────
        speed = car_data["Speed"] if "Speed" in car_data.columns else pd.Series(dtype=float)

        avg_speed_kph = safe_mean(speed)
        max_speed_kph = safe_max(speed)
        min_speed_kph = safe_min(speed)

        # ── Throttle ──────────────────────────────────────────────────────
        throttle = car_data["Throttle"] if "Throttle" in car_data.columns else pd.Series(dtype=float)

        avg_throttle_pct  = safe_mean(throttle)
        # Percentage of full throttle in the lap = samples where throttle >= 99%
        full_throttle_pct = safe_pct(throttle >= 99) if not throttle.empty else None

        # ── Brake ─────────────────────────────────────────────────────────
        # FastF1 returns Brake as bool (True=braking) or 0/1
        brake = car_data["Brake"] if "Brake" in car_data.columns else pd.Series(dtype=bool)
        heavy_braking_pct = safe_pct(brake) if not brake.empty else None

        # ── DRS ───────────────────────────────────────────────────────────
        # DRS values: 0=closed, 8=available, 10/12/14=open
        drs = car_data["DRS"] if "DRS" in car_data.columns else pd.Series(dtype=float)
        drs_active_pct = safe_pct(drs >= 10) if not drs.empty else None

        # ── Gear ──────────────────────────────────────────────────────────
        gear = car_data["nGear"] if "nGear" in car_data.columns else pd.Series(dtype=float)

        avg_gear = safe_mean(gear)

        # Gear changes = number of times gear value changes
        gear_changes = None
        if not gear.empty:
            gear_changes = safe_int((gear.diff().fillna(0) != 0).sum())

        # ── RPM ───────────────────────────────────────────────────────────
        rpm = car_data["RPM"] if "RPM" in car_data.columns else pd.Series(dtype=float)

        avg_rpm = safe_int(rpm.mean()) if not rpm.empty else None
        max_rpm = safe_int(rpm.max())  if not rpm.empty else None

        return {
            "driver_code":        driver_code,
            "lap_number":         lap_number,
            "sample_count":       len(car_data),      

            # Speed
            "avg_speed_kph":      avg_speed_kph,
            "max_speed_kph":      max_speed_kph,
            "min_speed_kph":      min_speed_kph,

            # Throttle
            "avg_throttle_pct":   avg_throttle_pct,
            "full_throttle_pct":  full_throttle_pct,

            # Brake
            "heavy_braking_pct":  heavy_braking_pct,

            # DRS
            "drs_active_pct":     drs_active_pct,

            # Gear
            "avg_gear":           avg_gear,
            "gear_changes":       gear_changes,

            # RPM
            "avg_rpm":            avg_rpm,
            "max_rpm":            max_rpm,
        }

    except Exception as e:
        logging.debug(f"  Lap aggregation failed D={driver_code} L={lap_number}: {e}")
        return None


# =============================================================================
# EXTRACT TELEMETRY FOR ONE RACE
# =============================================================================

def extract_race_telemetry(session: requests.Session,
                           year: int,
                           round_num: int,
                           race_info: dict,
                           driver_number_map: dict
                           ) -> pd.DataFrame:
    """
    Loop through every driver and every lap, fetch car telemetry,
    aggregate to one row per lap per driver.

    Strategy:
        1. Get all laps from session
        2. For each driver → for each lap → get_car_data()
        3. Aggregate raw samples → one summary row
        4. Skip inlaps, outlaps, laps with no telemetry

    Parameters:
        session    : loaded FastF1 session object
        year       : season year
        round_num  : race round number
        race_info  : dict with race_name, circuit_ref, race_date
    """
    laps = session.laps

    if laps is None or laps.empty:
        logging.warning(f"  No lap data in session for {year} R{round_num:02d}")
        return pd.DataFrame()

    all_records   = []
    drivers       = laps["Driver"].unique()
    total_drivers = len(drivers)

    logging.info(f"  Processing {total_drivers} drivers...")

    for d_idx, driver_code in enumerate(drivers, 1):
        logging.info(
            f"  Driver {d_idx}/{total_drivers}: {driver_code}"
        )

        # Get all laps for this driver
        driver_laps = laps.pick_drivers(driver_code)

        if driver_laps.empty:
            continue

        for _, lap in driver_laps.iterlaps():
            lap_number = safe_int(lap["LapNumber"])

            if lap_number is None:
                continue

            # ── Skip inlaps and outlaps ───────────────────────────────────
            # These have distorted telemetry (slow pit lane speeds etc.)
            # They would pollute your ML features
            is_inlap  = not pd.isnull(lap.get("PitInTime",  None))
            is_outlap = not pd.isnull(lap.get("PitOutTime", None))

            if is_inlap or is_outlap:
                logging.debug(
                    f"  Skipping {driver_code} L{lap_number} "
                    f"(inlap={is_inlap}, outlap={is_outlap})"
                )
                continue

            # ── Skip laps with no accurate timing ────────────────────────
            # FastF1 marks some laps as inaccurate (SC, red flag etc.)
            is_accurate = lap.get("IsAccurate", True)
            if not is_accurate:
                logging.debug(
                    f"  Skipping {driver_code} L{lap_number} (not accurate)"
                )
                continue

            # ── Fetch raw telemetry for this lap ──────────────────────────
            try:
                car_data = lap.get_car_data()
            except Exception as e:
                logging.debug(
                    f"  get_car_data failed {driver_code} L{lap_number}: {e}"
                )
                continue

            # ── Aggregate ─────────────────────────────────────────────────
            record = aggregate_lap_telemetry(car_data, driver_code, lap_number)

            if record is None:
                continue

            # ── Add race context ──────────────────────────────────────────
            record["season"]       = year
            record["round_number"] = round_num
            record["race_name"]    = race_info.get("race_name")
            record["circuit_ref"]  = race_info.get("circuit_ref")
            record["race_date"]    = race_info.get("race_date")
            record["driver_number"] = safe_int(                        
                driver_number_map.get(driver_code))

            all_records.append(record)

    if not all_records:
        logging.warning(
            f"  No telemetry records extracted for {year} R{round_num:02d}"
        )
        return pd.DataFrame()

    df = pd.DataFrame(all_records)

    # ── Column order ──────────────────────────────────────────────────────
    col_order = [
        "season", "round_number", "race_name", "circuit_ref", "race_date",
        "driver_code", "driver_number", "lap_number", "sample_count",
        "avg_speed_kph", "max_speed_kph", "min_speed_kph",
        "avg_throttle_pct", "full_throttle_pct",
        "heavy_braking_pct",
        "drs_active_pct",
        "avg_gear", "gear_changes",
        "avg_rpm", "max_rpm",
    ]
    col_order = [c for c in col_order if c in df.columns]
    df        = df[col_order]

    df = df.sort_values(
        ["driver_code", "lap_number"]
    ).reset_index(drop=True)

    return df


# =============================================================================
# SINGLE RACE TELEMETRY FETCH
# =============================================================================

def fetch_race_telemetry(session: requests.Session,
                         timeout: int,
                         year: int, 
                         round_num: int,
                         cache_dir: Path,
                         schedule_dir: Path) -> pd.DataFrame:
    """
    Load FastF1 race session and extract aggregated telemetry.

    FastF1 loads:
        laps=True      → needed to loop through laps and call get_car_data()
        telemetry=True → loads raw telemetry into cache
        weather=False  → not needed here
        messages=False → not needed here
    """
    race_info = get_race_info_from_disk(session=session, schedule_dir=schedule_dir, year=year, round_num=round_num, timeout=timeout)

    try:
        logging.info(f"  Loading FastF1 session {year} R{round_num:02d}...")
        fastf1_session = fastf1.get_session(year, round_num, "Q")
        fastf1_session.load(
            laps      = True,
            telemetry = True,  
            weather   = False,
            messages  = False,
        )
        logging.info(f"  Session loaded ✓")

        # Build driver_number_map from laps
        driver_number_map = (
            fastf1_session.laps[["Driver", "DriverNumber"]]
            .drop_duplicates()
            .set_index("Driver")["DriverNumber"]
            .to_dict()
        )  

    except Exception as e:
        logging.error(
            f"  FastF1 session load failed {year} R{round_num:02d}: {e}"
        )
        return pd.DataFrame()

    return extract_race_telemetry(fastf1_session, year, round_num, race_info, driver_number_map)


# =============================================================================
# MAIN INGESTION FUNCTION
# =============================================================================

def fetch_telemetry(session: requests.Session,
                    timeout: int,
                    schedule_dir: Path,
                    bronze_dir:   Path,
                    cache_dir:    Path,
                    start_year:   int,
                    end_year:     int,
                    force:        bool = False,
                    single_year:  int  = None,
                    single_round: int  = None) -> pd.DataFrame:
    """
    Fetch aggregated telemetry for all seasons and rounds via FastF1.
    Saves one parquet per race and one combined master file.

    Parameters:
        bronze_dir   : Path to bronze folder
        cache_dir    : Path to FastF1 cache folder
        start_year   : First season (minimum 2018)
        end_year     : Last season
        force        : Re-fetch even if files exist
        single_year  : Ingest only this season
        single_round : Ingest only this round within single_year
    """
    bronze_dir    = Path(bronze_dir)
    cache_dir     = Path(cache_dir)
    telemetry_dir = bronze_dir / "telemetry"
    telemetry_dir.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)

    # Enable FastF1 cache
    fastf1.Cache.enable_cache(str(cache_dir))
    logging.info(f"  FastF1 cache: {cache_dir}")

    master_path  = telemetry_dir / "all_seasons_telemetry.parquet"
    current_year = datetime.datetime.now().year
    new_dfs      = []

    # ── Determine year range ──────────────────────────────────────────────
    if single_year:
        year_range = [single_year]
    else:
        effective_start = max(start_year, config.START_YEAR)
        if start_year < config.START_YEAR:
            logging.warning(
                f"  Telemetry not available before {config.START_YEAR}. "
                f"Starting from {config.START_YEAR}."
            )
        year_range = range(effective_start, end_year + 1)

    for year in year_range:
        is_current_season = (year == current_year)

        logging.info(f"\n── Season {year} ──────────────────────────────")

        # ── Determine rounds ──────────────────────────────────────────────
        if single_round and single_year:
            rounds = [single_round]
        else:
            rounds = get_season_rounds(schedule_dir, year)

        if not rounds:
            logging.warning(f"  No rounds found for {year} — skipping")
            continue

        logging.info(f"  {len(rounds)} rounds to process")

        for round_num in rounds:
            save_path = telemetry_dir / f"{year}_R{round_num:02d}_telemetry.parquet"

            # ── Skip if already fetched ───────────────────────────────────
            if save_path.exists() and save_path.stat().st_size > 0 \
                    and not force and not is_current_season:
                logging.info(f"  R{round_num:02d} already exists — skipping")
                continue

            # ── Fetch ─────────────────────────────────────────────────────
            logging.info(f"  Fetching {year} R{round_num:02d} telemetry...")
            try:
                df_round = fetch_race_telemetry(
                    session=session, year=year, round_num=round_num, cache_dir=cache_dir, schedule_dir=schedule_dir, timeout=timeout
                )
            except Exception as e:
                logging.error(
                    f"  Failed {year} R{round_num:02d}: {e} — skipping"
                )
                continue

            if df_round.empty:
                logging.warning(
                    f"  No telemetry for {year} R{round_num:02d} — skipping"
                )
                continue

            # ── Save individual race file ─────────────────────────────────
            df_round.to_parquet(save_path, index=False, compression="snappy")
            logging.info(
                f"  Saved {year}_R{round_num:02d}_telemetry.parquet "
                f"({len(df_round):,} rows | "
                f"drivers={df_round['driver_code'].nunique()})"
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
            master = master[master["season"] != current_year]
            master = pd.concat([master, new_data], ignore_index=True)
            master = master.sort_values(
                ["season", "round_number", "driver_code", "lap_number"]
            ).reset_index(drop=True)

        else:
            logging.info("  Master not found — building from individual files...")
            all_files = sorted(telemetry_dir.glob("*_R*_telemetry.parquet"))

            if not all_files:
                logging.warning("  No telemetry parquet files found.")
                master = pd.DataFrame()
            else:
                master = pd.concat(
                    [pd.read_parquet(f) for f in all_files],
                    ignore_index=True
                ).sort_values(
                    ["season", "round_number", "driver_code", "lap_number"]
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
    
    df = fetch_telemetry(
        session      = SESSION,
        timeout      = config.TIMEOUT,
        schedule_dir = config.BRONZE_DIR / "schedule",
        bronze_dir   = config.BRONZE_DIR,
        cache_dir    = config.CACHE_DIR,
        start_year   = config.START_YEAR,
        end_year     = config.END_YEAR,
        force        = config.FORCE,
        single_year  = config.YEAR,
        single_round = config.ROUND,
    )

    print(f"\nShape        : {df.shape}")
    print(f"Seasons      : {sorted(df['season'].unique())}")
    print(f"Columns      : {list(df.columns)}")
    
    print("\nTELEMETRY DATA:")
    logging.info(df.head())
    print(df.head())
