import logging
from pathlib import Path
from datetime import datetime

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
LOGS_DIR = ROOT_DIR / "data" / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# ── Log file ──────────────────────────────────────────────────────────────────
LOG_FILE      = f"{datetime.now().strftime('%m_%d_%Y_%H_%M_%S')}.log"
LOG_FILE_PATH = LOGS_DIR / LOG_FILE

# ── Configure ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    filename = str(LOG_FILE_PATH),
    format   = "[ %(asctime)s ] %(lineno)d %(name)s - %(levelname)s - %(message)s",
    level    = logging.INFO
)