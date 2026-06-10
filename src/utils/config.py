from pathlib import Path

START_YEAR = 2018
END_YEAR = 2025
FORCE = False
YEAR = None # used in ingest_laps
ROUND = None # used in ingest_laps
CACHE_DIR = Path('data/cache') # used in ingest_weather
CACHE_DIR_TELEMETRY = Path('data/cache_telemetry')
INTER_SEASON_DELAY   = 3.0    # pause between seasons
BASE_URL     = "https://api.jolpi.ca/ergast/f1"
TIMEOUT = 30
RATE_LIMIT_DELAY  = 1.0   # seconds between every API call
BRONZE_DIR = Path(r'C:\Users\Asus\Desktop\Formula1\data\bronze')
PAGE_LIMIT = 100 # used in ingest_laps
PIT_DATA_START_YEAR  = 2018 # used in ingest_pitstop
DATA_DIR = Path(r"C:\Users\Asus\Desktop\Formula1\data")