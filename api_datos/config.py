from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv()

BASE_DIR = Path(__file__).parent

# ── Datos locales
DATA_DIR = Path(os.getenv("DATA_DIR", BASE_DIR / "data"))

DRIVE_FILES = {
    "pools":     "pool_list_complete.csv",
    "events":    "eventos_pool_sync_mint_burn.csv",
    "metadata":  "token_metadata_complete.csv",
    "transfers": "eventos_transfers_tokens.csv",
}

# ── Rutas locales
STATE_DIR  = BASE_DIR / "state"
STATE_FILE = STATE_DIR / "batch_state.json"
IDS_FILE   = STATE_DIR / "file_ids.json"   

# ── Batches
NUM_BATCHES = int(os.getenv("NUM_BATCHES", 12))