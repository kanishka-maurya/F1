import datetime
import logging
import time
import requests
import pandas as pd
import fastf1
from pathlib import Path
from src.utils import config
from src.utils.logger import logging
from src.utils.utility import build_session, get_season_rounds, get_race_info_from_disk


# Enable FastF1 cache
_CACHE_DIR = Path(config.BRONZE_DIR) / ".fastf1_cache"
_CACHE_DIR.mkdir(parents=True, exist_ok=True)
fastf1.Cache.enable_cache(str(_CACHE_DIR))


# =============================================================================
# FETCH QUALIFYING LAPS FOR ONE RACE
# =============================================================================

def fetch_all_quali_laps(year: int, round_num: int) -> pd.DataFrame:
    """
    Load all qualifying lap data for a single race via FastF1.

    FastF1 returns a Laps DataFrame for the full qualifying session.
    We use split_qualifying_sessions() to assign each lap to Q1 / Q2 / Q3.

    Returns:
        Raw FastF1 Laps DataFrame with an added 'quali_session' column
        ("Q1", "Q2", or "Q3"), or an empty DataFrame if data is unavailable.
    """
    try:
        session = fastf1.get_session(year, round_num, "Q")
        session.load(laps=True, telemetry=False, weather=False, messages=True)
    except Exception as e:
        logging.warning(f"  Could not load FastF1 session for {year} R{round_num:02d}: {e}")
        return pd.DataFrame()

    try:
        q1_laps, q2_laps, q3_laps = session.laps.split_qualifying_sessions()
    except Exception as e:
        logging.warning(
            f"  Could not split qualifying sessions for {year} R{round_num:02d}: {e}"
            f" — tagging all laps as Q_UNKNOWN"
        )
        # Fall back: return all laps without Q tagging
        df = session.laps.copy().reset_index(drop=True)
        df["quali_session"] = "Q_UNKNOWN"
        return df

    parts = []
    for label, laps in [("Q1", q1_laps), ("Q2", q2_laps), ("Q3", q3_laps)]:
        if laps is not None and not laps.empty:
            chunk = laps.copy().reset_index(drop=True)
            chunk["quali_session"] = label
            parts.append(chunk)

    if not parts:
        logging.warning(f"  No qualifying lap data found for {year} R{round_num:02d}")
        return pd.DataFrame()

    all_laps = pd.concat(parts, ignore_index=True)
    logging.info(
        f"  FastF1: {len(all_laps)} total quali laps fetched for "
        f"{year} R{round_num:02d}"
    )
    return all_laps


# =============================================================================
# EXTRACT — flatten FastF1 laps into target schema
# =============================================================================

def extract_quali_laps(
    raw_laps: pd.DataFrame,
    year: int,
    round_num: int,
    race_info: dict,
    driver_ref_map: dict,
) -> pd.DataFrame:
    """
    Transform FastF1 Laps DataFrame into the project schema.

    Output columns (one row per driver per lap):
        season          int
        round_number    int
        race_name       str
        circuit_ref     str
        race_date       date
        driver_ref      str    ← three-letter abbreviation lowercased
        lap_number      Int16
        position        Int16  ← position at end of that lap
        lap_time        str    ← formatted as M:SS.mmm  (NaT → None)
        lap_time_ms     float  ← milliseconds (NaT → NaN)
        quali_session   str    ← "Q1", "Q2", or "Q3"
        sector1_time_ms float
        sector2_time_ms float
        sector3_time_ms float
        is_personal_best bool
        compound        str
        deleted         bool
        deleted_reason  str
    """
    if raw_laps.empty:
        return pd.DataFrame()

    def timedelta_to_ms(td) -> float | None:
        """Convert a pandas Timedelta to float milliseconds, or NaN."""
        try:
            if pd.isna(td):
                return float("nan")
        except (TypeError, ValueError):
            pass
        if isinstance(td, pd.Timedelta):
            return td.total_seconds() * 1000
        return float("nan")

    def timedelta_to_str(td) -> str | None:
        """Format a Timedelta as M:SS.mmm string."""
        try:
            if pd.isna(td):
                return None
        except (TypeError, ValueError):
            pass
        if not isinstance(td, pd.Timedelta):
            return None
        total_seconds = td.total_seconds()
        minutes = int(total_seconds // 60)
        seconds = total_seconds % 60
        return f"{minutes}:{seconds:06.3f}"

    records = []
    for _, row in raw_laps.iterrows():
        driver_abbr = str(row.get("Driver", "")).lower()
        # Prefer the ergast-style driver_ref from the lookup map if available
        driver_ref  = driver_ref_map.get(driver_abbr, driver_abbr)

        records.append({
            "season":            year,
            "round_number":      round_num,
            "race_name":         race_info.get("race_name"),
            "circuit_ref":       race_info.get("circuit_ref"),
            "race_date":         race_info.get("race_date"),
            "driver_ref":        driver_ref,
            "lap_number":        row.get("LapNumber"),
            "position":          row.get("Position"),
            "lap_time":          timedelta_to_str(row.get("LapTime")),
            "lap_time_ms":       timedelta_to_ms(row.get("LapTime")),
            "quali_session":     row.get("quali_session"),
            "sector1_time_ms":   timedelta_to_ms(row.get("Sector1Time")),
            "sector2_time_ms":   timedelta_to_ms(row.get("Sector2Time")),
            "sector3_time_ms":   timedelta_to_ms(row.get("Sector3Time")),
            "is_personal_best":  row.get("IsPersonalBest"),
            "compound":          row.get("Compound"),
            "deleted":           row.get("Deleted"),
            "deleted_reason":    row.get("DeletedReason"),
        })

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)

    # ── Type casting ──────────────────────────────────────────────────────
    df["lap_number"]  = pd.to_numeric(df["lap_number"], errors="coerce").astype("Int16")
    df["position"]    = pd.to_numeric(df["position"],   errors="coerce").astype("Int16")
    df["race_date"]   = pd.to_datetime(df["race_date"],  errors="coerce").dt.date

    # Sort by quali session first, then lap number, then position
    session_order = {"Q1": 0, "Q2": 1, "Q3": 2, "Q_UNKNOWN": 3}
    df["_q_order"] = df["quali_session"].map(session_order).fillna(9)
    df = (df.sort_values(["_q_order", "lap_number", "position"])
            .drop(columns=["_q_order"])
            .reset_index(drop=True))

    return df


# =============================================================================
# DRIVER REF MAP — build from FastF1 session results
# =============================================================================

def build_driver_ref_map(year: int, round_num: int) -> dict:
    """
    Return a mapping of {three-letter-abbr.lower() -> driver_ref} for the
    given session. FastF1 results carry 'Abbreviation' and optionally an
    ergast-style HeadshotUrl path we can parse, but the simplest reliable
    key is just the lowercase abbreviation itself (e.g. "ver", "lec").

    If the session cannot be loaded, returns an empty dict and the caller
    will fall back to using the raw abbreviation as driver_ref.
    """
    try:
        session = fastf1.get_session(year, round_num, "Q")
        # results is lightweight — only loads driver info
        session.load(laps=False, telemetry=False, weather=False, messages=False)
        results = session.results
        if results is None or results.empty:
            return {}
        # 'Abbreviation' column holds e.g. "VER", "LEC"
        mapping = {
            str(row["Abbreviation"]).lower(): str(row["Abbreviation"]).lower()
            for _, row in results.iterrows()
            if pd.notna(row.get("Abbreviation"))
        }
        return mapping
    except Exception:
        return {}


# =============================================================================
# SINGLE RACE INGESTION
# =============================================================================

def fetch_race_quali_laps(
    session: requests.Session,
    schedule_dir: Path,
    year: int,
    round_num: int,
    timeout: int,
) -> pd.DataFrame:
    """
    Fetch, flatten, and return all qualifying lap data for one race.
    """
    race_info      = get_race_info_from_disk(
        session=session, timeout=timeout,
        schedule_dir=schedule_dir, year=year, round_num=round_num,
    )
    driver_ref_map = build_driver_ref_map(year, round_num)
    raw_laps       = fetch_all_quali_laps(year, round_num)

    if raw_laps.empty:
        return pd.DataFrame()

    df = extract_quali_laps(raw_laps, year, round_num, race_info, driver_ref_map)
    logging.info(
        f"  Extracted {len(df)} rows for "
        f"{year} Q{round_num:02d} ({race_info.get('race_name', '?')})"
    )
    return df


# =============================================================================
# MAIN INGESTION FUNCTION
# =============================================================================

def fetch_qualifying_lap_data(
    session: requests.Session,
    schedule_dir: Path,
    bronze_dir: Path,
    start_year: int,
    end_year: int,
    timeout: int,
    force: bool = False,
    single_year: int = None,
    single_round: int = None,
) -> pd.DataFrame:
    """
    Fetch qualifying lap data for all seasons and rounds.
    Saves one parquet per race and one combined master file.

    Parameters:
        bronze_dir   : Path to bronze folder
        start_year   : First season to ingest
        end_year     : Last season to ingest
        timeout      : Request timeout in seconds
        force        : Re-fetch all races even if files exist
        single_year  : Ingest only this season (overrides start/end)
        single_round : Ingest only this round within single_year
    """
    bronze_dir   = Path(bronze_dir)
    quali_dir    = bronze_dir / "qualifying_laps"
    quali_dir.mkdir(parents=True, exist_ok=True)

    master_path  = quali_dir / "all_seasons_qualifying_laps.parquet"
    current_year = datetime.datetime.now().year
    new_dfs      = []

    # ── Determine year range ──────────────────────────────────────────────
    year_range = [single_year] if single_year else range(start_year, end_year + 1)

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
            # ── File uses "Q" prefix instead of "R" ──────────────────────
            save_path = quali_dir / f"{year}_Q{round_num:02d}_qualifying_laps.parquet"

            # ── Skip if already fetched ───────────────────────────────────
            if (
                save_path.exists()
                and save_path.stat().st_size > 0
                and not force
                and not is_current_season
            ):
                logging.info(f"  Q{round_num:02d} already exists — skipping")
                continue

            # ── Fetch ─────────────────────────────────────────────────────
            logging.info(f"  Fetching {year} Q{round_num:02d} qualifying laps...")
            try:
                df_round = fetch_race_quali_laps(
                    session=session,
                    schedule_dir=schedule_dir,
                    year=year,
                    round_num=round_num,
                    timeout=timeout,
                )
            except Exception as e:
                logging.error(
                    f"  Failed {year} Q{round_num:02d}: {e} — skipping"
                )
                continue

            if df_round.empty:
                logging.warning(
                    f"  No qualifying lap data for {year} Q{round_num:02d} — skipping"
                )
                continue

            # ── Save individual race file ─────────────────────────────────
            df_round.to_parquet(save_path, index=False, compression="snappy")
            logging.info(
                f"  Saved {year}_Q{round_num:02d}_qualifying_laps.parquet "
                f"({len(df_round):,} rows)"
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

            # Remove current season to avoid duplicates
            master = master[master["season"] != current_year]

            master = pd.concat([master, new_data], ignore_index=True)
            master = master.sort_values(
                ["season", "round_number", "quali_session", "lap_number", "position"]
            ).reset_index(drop=True)

        else:
            # Build master from all individual files
            logging.info("  Master not found — building from individual files...")
            all_files = sorted(quali_dir.glob("*_Q*_qualifying_laps.parquet"))

            if not all_files:
                logging.warning("  No qualifying parquet files found.")
                master = pd.DataFrame()
            else:
                master = pd.concat(
                    [pd.read_parquet(f) for f in all_files],
                    ignore_index=True,
                ).sort_values(
                    ["season", "round_number", "quali_session", "lap_number", "position"]
                ).reset_index(drop=True)

        master.to_parquet(master_path, index=False, compression="snappy")
        logging.info(f"  Master updated — {len(master):,} total rows")

    else:
        # ── No new data ───────────────────────────────────────────────────
        if not master_path.exists():
            logging.warning(
                "  No new data and no master file — returning empty | Set FORCE = True."
            )
            return pd.DataFrame()

        logging.info("  No new rounds fetched — master unchanged, loading from disk")
        master = pd.read_parquet(master_path)

    return master


# ── Run ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    from src.utils.utility import build_session

    SESSION = build_session()

    df = fetch_qualifying_lap_data(
        session      = SESSION,
        schedule_dir = config.BRONZE_DIR / "schedule",
        bronze_dir   = config.BRONZE_DIR,
        start_year   = config.START_YEAR,
        end_year     = config.END_YEAR,
        timeout      = config.TIMEOUT,
        force        = config.FORCE,
        single_year  = config.YEAR,
        single_round = config.ROUND,
    )

    print(f"\nShape          : {df.shape}")
    print(f"Seasons        : {sorted(df['season'].unique())}")
    print(f"Columns        : {list(df.columns)}")
    print(f"Quali sessions : {df['quali_session'].value_counts().to_dict()}")

    print("\nQUALIFYING LAPS DATA:")
    logging.info(df.head())
    print(df.head())