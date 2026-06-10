import pandas as pd
import requests
from pathlib import Path
from src.utils import config
from src.utils.logger import logging
from src.utils.utility import safe_get, build_session


# =============================================================================
# NEW DATA CHECKER — tells if new data added to api
# =============================================================================

def has_new_data(save_path: Path, endpoint: str, session: requests.Session, timeout: int) -> bool:
    """
    Hit the API with limit=1 just to get the total count.
    Compare against rows saved on disk.
    Returns True if API has more records than local file.
    """

    try:
        url      = f"{config.BASE_URL}{endpoint}?limit=1&offset=0"
        response = safe_get(session=session, url=url, timeout=timeout)
        response.raise_for_status()
        api_total  = int(response.json()["MRData"]["total"])
        disk_total = len(pd.read_parquet(save_path))

        if api_total > disk_total:
            logging.info(f"  New data detected — API: {api_total}, disk: {disk_total}")
            return True

        if api_total < disk_total:
            logging.warning(f"  Disk has more rows ({disk_total}) than API ({api_total}) — investigate")

        logging.info(f"  No new data — {disk_total} records up to date")
        return False

    except Exception as e:
        logging.warning(f"  Could not check for new data: {e} — will re-fetch")
        return True 


# =============================================================================
# FETCH FUNCTION
# =============================================================================

def fetch_entity(
    session: requests.Session,
    timeout: int,
    bronze_dir: Path,
    endpoint: str,
    table_key: str,
    list_key: str,
    extract_fn,
    save_name: str,
    force: bool = False,
   
    ) -> pd.DataFrame:


    """
 
    Parameters:
        session     : Session object
        timeout     : timeout of server
        bronze_dir  : Path to bronze folder
        endpoint    : API endpoint e.g. '/drivers.json'
        table_key   : JSON key for the table e.g. 'DriverTable'
        list_key    : JSON key for the list  e.g. 'Drivers'
        extract_fn  : function(record) → dict of fields to keep
        save_name   : parquet filename e.g. 'drivers.parquet'
        force       : re-fetch even if file exists
    """

    bronze_dir = Path(bronze_dir)
    save_path  = bronze_dir / "entities" / save_name
    save_path.parent.mkdir(parents=True, exist_ok=True)

    # ── Idempotency ───────────────────────────────────────────────────────
    if save_path.exists() and not force:
        if not has_new_data(save_path, endpoint, session=session, timeout=timeout):
            return pd.read_parquet(save_path)
        logging.info(f"  Updating {save_name} with new records...")

    # ── Paginated fetch ───────────────────────────────────────────────────
    logging.info(f"  Fetching {save_name} from Jolpica...")
    all_records = []
    limit       = 100
    offset      = 0

    while True:
        url = f"{config.BASE_URL}{endpoint}?limit={limit}&offset={offset}"
        try:
            response = safe_get(session=session, url=url, timeout=timeout)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            logging.error(f"  API call failed at offset {offset}: {e}")
            raise
 
        records = data["MRData"][table_key][list_key]
        total   = int(data["MRData"]["total"])
 
        all_records.extend(records)
        logging.info(f"  Fetched {len(all_records)}/{total}...")
 
        offset += limit
        if offset >= total:
            break
 
    logging.info(f"  Total fetched: {len(all_records)}")

    # ── Extract fields using the provided function ─────────────────────────
    df = pd.DataFrame([extract_fn(r) for r in all_records])
 
    # ── Save ──────────────────────────────────────────────────────────────
    df.to_parquet(save_path, index=False, compression="snappy")
    logging.info(f"  Saved {len(df)} rows → {save_path}")
 
    return df


# =============================================================================
# EXTRACT FUNCTIONS — one per entity
# =============================================================================

def extract_driver(d: dict) -> dict:
    return {
        "driver_ref":    d.get("driverId"),
        "driver_code":   d.get("code"),
        "driver_number": d.get("permanentNumber"),
        "forename":      d.get("givenName"),
        "surname":       d.get("familyName"),
        "date_of_birth": d.get("dateOfBirth"),
        "nationality":   d.get("nationality"),
    }
 
 
def extract_constructor(c: dict) -> dict:
    return {
        "constructor_ref":  c.get("constructorId"),
        "constructor_name": c.get("name"),
        "nationality":      c.get("nationality"),
    }
 
 
def extract_circuit(c: dict) -> dict:
    location = c.get("Location", {})   # location is a nested dict
    return {
        "circuit_ref":  c.get("circuitId"),
        "circuit_name": c.get("circuitName"),
        "city":         location.get("locality"),
        "country":      location.get("country"),
        "latitude":     location.get("lat"),
        "longitude":    location.get("long"),
        "altitude_m":   location.get("alt"),
    }


# =============================================================================
# MAIN — fetch all three entities
# =============================================================================
 
def fetch_all_entities(bronze_dir: Path, session: requests.Session, timeout: int, force: bool = False) -> dict:
    """
    Fetch drivers, constructors, and circuits.
    Returns a dict with all three DataFrames.
    """
    bronze_dir = Path(bronze_dir)
 
    logging.info("=" * 50)
    logging.info("FETCHING ALL ENTITIES")
    logging.info("=" * 50)
 
    # ── Drivers ───────────────────────────────────────────────────────────
    logging.info("\n── Drivers ──────────────────────────────────")
    drivers = fetch_entity(
        session    = session,
        timeout    = timeout,
        bronze_dir = bronze_dir,
        endpoint   = "/drivers.json",
        table_key  = "DriverTable",
        list_key   = "Drivers",
        extract_fn = extract_driver,
        save_name  = "drivers.parquet",
        force      = force,
    )
    logging.info(f"  Final driver count: {len(drivers)}")
 
    # ── Constructors ──────────────────────────────────────────────────────
    logging.info("\n── Constructors ─────────────────────────────")
    constructors = fetch_entity(
        session    = session,
        timeout    = timeout,
        bronze_dir = bronze_dir,
        endpoint   = "/constructors.json",
        table_key  = "ConstructorTable",
        list_key   = "Constructors",
        extract_fn = extract_constructor,
        save_name  = "constructors.parquet",
        force      = force,
    )
    logging.info(f"  Final constructor count: {len(constructors)}")
 
    # ── Circuits ──────────────────────────────────────────────────────────
    logging.info("\n── Circuits ─────────────────────────────────")
    circuits = fetch_entity(
        session    = session,
        timeout    = timeout,
        bronze_dir = bronze_dir,
        endpoint   = "/circuits.json",
        table_key  = "CircuitTable",
        list_key   = "Circuits",
        extract_fn = extract_circuit,
        save_name  = "circuits.parquet",
        force      = force,
    )
    logging.info(f"  Final circuit count: {len(circuits)}")
 
    # ── Summary ───────────────────────────────────────────────────────────
    logging.info("\n" + "=" * 50)
    logging.info("DONE")
    logging.info(f"  Drivers      : {len(drivers)}")
    logging.info(f"  Constructors : {len(constructors)}")
    logging.info(f"  Circuits     : {len(circuits)}")
    logging.info("=" * 50)
 
    return {
        "drivers":      drivers,
        "constructors": constructors,
        "circuits":     circuits,
    }
 

# ── Run ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # BUILD SESSION
    SESSION = build_session()

    results = fetch_all_entities(
                                bronze_dir=config.BRONZE_DIR,
                                force=config.FORCE,
                                session= SESSION,
                                timeout=config.TIMEOUT)
 
    print("\nDrivers:")
    print(results["drivers"].head())
 
    print("\nConstructors:")
    print(results["constructors"].head())
 
    print("\nCircuits:")
    print(results["circuits"].head())
 