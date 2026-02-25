import json
import logging
from datetime import datetime, timezone, timedelta

from config import NUM_BATCHES, STATE_FILE, STATE_DIR
from services import db, drive

logger = logging.getLogger(__name__)

STATE_DIR.mkdir(parents=True, exist_ok=True)

# ── Referencia Ethereum block → fecha ─
# Bloque 10,000,000 minado el 11 jun 2020 00:00:00 UTC (aprox)
ETH_REF_BLOCK     = 10_000_000
ETH_REF_TIMESTAMP = datetime(2020, 6, 11, 0, 0, 0, tzinfo=timezone.utc)
ETH_BLOCK_SECONDS = 13.2   # segundos promedio por bloque en esa época


def block_to_datetime(block: int) -> datetime:
    """Convierte un block number a datetime UTC aproximado."""
    delta_blocks = block - ETH_REF_BLOCK
    delta_seconds = delta_blocks * ETH_BLOCK_SECONDS
    return ETH_REF_TIMESTAMP + timedelta(seconds=delta_seconds)


def block_to_month_label(block: int) -> str:
    """Retorna 'YYYY-MM' del bloque dado."""
    dt = block_to_datetime(block)
    return dt.strftime("%Y-%m")


def month_label(year: int, month: int) -> str:
    return f"{year:04d}-{month:02d}"


def next_month(year: int, month: int) -> tuple[int, int]:
    if month == 12:
        return year + 1, 1
    return year, month + 1


# ── Persistencia de estado

def _load() -> dict:
    if STATE_FILE.exists() and STATE_FILE.stat().st_size > 0:
        with open(STATE_FILE) as f:
            return json.load(f)
    return {}


def _save(state: dict):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


# ── Inicialización

def initialize() -> dict:
    """
    Calcula el rango de bloques del dataset, lo convierte a meses calendario
    y crea NUM_BATCHES (12) ventanas mensuales consecutivas.

    Si el dataset cubre más de 12 meses, toma los primeros 12.
    Si cubre menos, crea tantos batches como meses haya.
    """
    state = _load()
    if state.get("initialized"):
        logger.info("Batcher ya inicializado.")
        return state

    urls = drive.get_urls()
    logger.info("Calculando rango de bloques desde Drive...")
    block_min, block_max = db.get_block_range(urls["events"])

    # Convertir a fechas
    dt_min = block_to_datetime(block_min)
    dt_max = block_to_datetime(block_max)
    logger.info(
        f"Rango temporal: {dt_min.strftime('%b %Y')} → {dt_max.strftime('%b %Y')} "
        f"(bloques {block_min:,} → {block_max:,})"
    )

    # Construir lista de meses consecutivos desde el primero del dataset
    start_year  = dt_min.year
    start_month = dt_min.month
    batches = []

    year, month = start_year, start_month
    for i in range(NUM_BATCHES):
        # Primer bloque del mes (aproximado)
        month_start_dt = datetime(year, month, 1, 0, 0, 0, tzinfo=timezone.utc)
        delta = (month_start_dt - ETH_REF_TIMESTAMP).total_seconds()
        b_start = ETH_REF_BLOCK + int(delta / ETH_BLOCK_SECONDS)
        b_start = max(b_start, block_min if i == 0 else b_start)

        # Último bloque del mes = primer bloque del mes siguiente - 1
        ny, nm = next_month(year, month)
        month_end_dt = datetime(ny, nm, 1, 0, 0, 0, tzinfo=timezone.utc)
        delta_end = (month_end_dt - ETH_REF_TIMESTAMP).total_seconds()
        b_end = ETH_REF_BLOCK + int(delta_end / ETH_BLOCK_SECONDS) - 1
        b_end = min(b_end, block_max)

        batches.append({
            "id":          i + 1,
            "month":       month_label(year, month),
            "block_start": b_start,
            "block_end":   b_end,
        })

        logger.info(
            f"  Batch {i+1:2d}: {month_label(year, month)} | "
            f"bloques {b_start:,} → {b_end:,}"
        )

        # Si ya cubrimos hasta el final del dataset, parar
        if b_end >= block_max:
            break

        year, month = ny, nm

    num_created = len(batches)
    state = {
        "initialized":   True,
        "current_batch": 1,
        "num_batches":   num_created,
        "block_min":     block_min,
        "block_max":     block_max,
        "date_min":      dt_min.strftime("%Y-%m-%d"),
        "date_max":      dt_max.strftime("%Y-%m-%d"),
        "batches":       batches,
    }
    _save(state)
    logger.info(f"✓ {num_created} batches mensuales creados.")
    return state


# ── Consultas

def get_status() -> dict:
    state = _load()
    if not state.get("initialized"):
        return {"initialized": False}
    return state


def _require_init() -> dict:
    state = _load()
    if not state.get("initialized"):
        raise RuntimeError("API no inicializada. Llama a POST /init primero.")
    return state


def get_meta(batch_id: int) -> dict:
    state = _require_init()
    for b in state["batches"]:
        if b["id"] == batch_id:
            return b
    raise ValueError(f"Batch {batch_id} no existe.")


def get_current_id() -> int:
    state = _require_init()
    return state["current_batch"]


def advance() -> int:
    """Avanza el puntero al siguiente batch. Al llegar al último, vuelve al 1."""
    state = _require_init()
    served      = state["current_batch"]
    num_batches = state["num_batches"]
    state["current_batch"] = (served % num_batches) + 1
    _save(state)
    logger.info(f"Batch {served} ({get_meta(served)['month']}) servido → siguiente: {state['current_batch']}")
    return served


# ── Extracción de datos 

def fetch(batch_id: int) -> dict:
    """
    Extrae los 4 datasets filtrados al rango de bloques del mes.
    Todo vía DuckDB + httpfs → sin archivos en disco.
    """
    meta    = get_meta(batch_id)
    urls    = drive.get_urls()
    b_start = meta["block_start"]
    b_end   = meta["block_end"]

    logger.info(
        f"Consultando batch {batch_id} | {meta['month']} | "
        f"bloques {b_start:,} → {b_end:,}"
    )

    events        = db.query_events(urls["events"], b_start, b_end)
    active_pairs  = db.get_active_pairs(urls["events"], b_start, b_end)
    pools         = db.query_pools(urls["pools"], active_pairs)
    active_tokens = db.get_active_tokens(urls["pools"], active_pairs)
    metadata      = db.query_metadata(urls["metadata"], active_tokens)
    transfers     = db.query_transfers(urls["transfers"], b_start, b_end)

    event_types = {}
    for ev in events:
        t = ev.get("event_type", "unknown")
        event_types[t] = event_types.get(t, 0) + 1

    logger.info(
        f"Batch {batch_id} ({meta['month']}): "
        f"{len(pools)} pools | {len(events):,} eventos | {len(transfers):,} transfers"
    )

    return {
        "meta": meta,
        "summary": {
            "month":             meta["month"],
            "n_pools":           len(pools),
            "n_events":          len(events),
            "n_events_by_type":  event_types,
            "n_metadata_rows":   len(metadata),
            "n_transfers":       len(transfers),
        },
        "data": {
            "pools":     pools,
            "events":    events,
            "metadata":  metadata,
            "transfers": transfers,
        },
    }


# ── Reset 

def reset():
    if STATE_FILE.exists():
        STATE_FILE.unlink()
    logger.info("Estado reseteado.")