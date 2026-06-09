import requests
import time
import pandas as pd
from pathlib import Path
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from src.utils.logger import logging
from src.utils import config





def build_session() -> requests.Session:
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
        read=3,
        connect=3,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        respect_retry_after_header=True,
        raise_on_status=False, 
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session





def safe_get(session: requests.Session, url: str, timeout: int) -> requests.Response:
    """
    GET with:
      - Inter-request delay (RATE_LIMIT_DELAY)
      - urllib3 handles all retries: 429s, 5xx, timeouts
      - raises on non-2xx after all retries exhausted
      - raises on timeout after all retries exhausted
    """
    time.sleep(config.RATE_LIMIT_DELAY)

    try:
        response = session.get(url, timeout=timeout)
        response.raise_for_status()
        return response

    except requests.exceptions.Timeout:
        logging.error(f"Request to {url} timed out after all retries.")
        raise

    except requests.exceptions.HTTPError as e:
        logging.error(f"HTTP error for {url}: {e.response.status_code}")
        raise

    except requests.exceptions.ConnectionError:
        logging.error(f"Connection error for {url} — network issue?")
        raise





def get_season_rounds(schedule_dir: Path, year: int) -> list[int]:
    """
    Read round numbers from saved schedule parquet — zero API calls.
    """
    schedule_path = schedule_dir / f"{year}_schedule.parquet"

    if not schedule_path.exists():
        logging.error(
            f"Schedule file not found for {year}. "
            f"Run ingest_schedule.py first."
        )
        return []

    try:
        df = pd.read_parquet(schedule_path)
        rounds = sorted(df["round_number"].dropna().astype(int).tolist())
        logging.info(f"  Loaded {len(rounds)} rounds from disk schedule")
        return rounds
    except Exception:
        logging.exception(f"  Failed to read schedule for {year}")
        raise





def parse_laptime_to_ms(laptime_str: str) -> float | None:
    """
    Convert a lap time string to milliseconds.

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
    




def timedelta_to_ms(td) -> float | None:
    """Convert pandas Timedelta to milliseconds. Returns None if NaT."""
    try:
        if pd.isnull(td):
            return None
        return round(td.total_seconds() * 1000, 1)
    except Exception:
        return None
    




def get_race_info_from_disk(schedule_dir: Path,
                            year: int, round_num: int) -> dict:
    """Read race name and circuit from saved schedule parquet."""
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
    return {}