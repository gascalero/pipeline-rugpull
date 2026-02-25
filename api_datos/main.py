import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from config import NUM_BATCHES
from services import batcher
from services import drive, db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(" Arrancando Rug Pull Batch API (DuckDB + Drive)...")
    status = batcher.get_status()
    if status["initialized"]:
        logger.info(f"Estado cargado. Próximo batch: {status['current_batch']}")
    else:
        logger.info("Sin estado. Llama POST /init para inicializar.")
    yield


app = FastAPI(
    title="Rug Pull Batch API",
    description=(
        "Sirve los datos de pools Uniswap V2 en 10 batches temporales. "
        "Lectura directa desde Google Drive via DuckDB httpfs — sin descargas."
    ),
    version="2.0.0",
    lifespan=lifespan,
)


# ── Helpers

def _json_safe(obj):
    """Convierte tipos no serializables de pandas/numpy a tipos nativos Python."""
    import pandas as pd
    import numpy as np
    if isinstance(obj, dict):
        return {k: _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_json_safe(i) for i in obj]
    if isinstance(obj, float) and (obj != obj):  # NaN
        return None
    if isinstance(obj, (pd.Timestamp, pd.NaT.__class__)):
        return str(obj) if not pd.isna(obj) else None
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        return None if np.isnan(obj) else float(obj)
    if isinstance(obj, (np.bool_,)):
        return bool(obj)
    if pd.isna(obj) if not isinstance(obj, (list, dict, str, bool)) else False:
        return None
    return obj


def _apply_limit(data: dict, limit: int) -> dict:
    """Trunca cada lista de datos al límite indicado. limit=0 → sin límite."""
    if limit == 0:
        return data
    return {key: rows[:limit] for key, rows in data.items()}


def _preview_data(data: dict, active: bool) -> dict:
    """Si preview=True recorta cada lista a 10 filas."""
    if not active:
        return data
    return {k: v[:10] if isinstance(v, list) else v for k, v in data.items()}


def _build_response(batch_data: dict, batch_id: int, next_id: int | None = None, preview: bool = False) -> dict:
    meta = batch_data["meta"]
    resp = {
        "batch_id":    batch_id,
        "batch_total": NUM_BATCHES,
        "month":       meta.get("month"),
        "block_start": meta["block_start"],
        "block_end":   meta["block_end"],
        "served_at":   datetime.now(timezone.utc).isoformat(),
        "preview":     preview,
        "summary":     batch_data["summary"],
        "data":        _json_safe(_preview_data(batch_data["data"], preview)),
    }
    if next_id is not None:
        resp["next_batch_id"] = next_id
    return resp


# ── Endpoints

@app.post("/init", summary="Inicializa los 10 batches temporales")
def init():
    """
    Consulta el rango de block_number desde Drive via DuckDB y divide
    en 10 ventanas temporales iguales. Si ya está inicializado, no hace nada.
    """
    try:
        state = batcher.initialize()
        return {"message": "Inicializado correctamente.", "status": state}
    except Exception as e:
        logger.error(f"/init error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/status", summary="Estado actual del ciclo")
def status():
    s = batcher.get_status()
    if not s["initialized"]:
        raise HTTPException(status_code=400, detail="No inicializada. Llama POST /init.")
    return s


@app.get("/batch/next", summary="Siguiente batch en el ciclo — endpoint de Airflow")
def next_batch(preview: bool = False):
    """
    Retorna los datos del siguiente batch y avanza el puntero.

    Ciclo: 1 → 2 → ... → 12 → 1 → 2 → ... (infinito, sin repetir en la vuelta)

    - **preview=false** *(default)*: datos completos — para Airflow.
    - **preview=true**: primeras 10 filas por dataset — para explorar en Swagger.
    """
    try:
        batch_id   = batcher.get_current_id()
        batch_data = batcher.fetch(batch_id)
        batcher.advance()
        next_id    = batcher.get_current_id()
        return JSONResponse(content=_build_response(batch_data, batch_id, next_id, preview=preview))
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"/batch/next error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/batch/{batch_id}", summary="Batch específico sin mover el puntero")
def get_batch(batch_id: int, preview: bool = False):
    """
    Retorna los datos de un mes específico sin alterar el ciclo.

    - **preview=false** *(default)*: datos completos.
    - **preview=true**: primeras 10 filas por dataset.
    """
    if not 1 <= batch_id <= NUM_BATCHES:
        raise HTTPException(status_code=422, detail=f"batch_id debe ser 1–{NUM_BATCHES}.")
    try:
        batch_data = batcher.fetch(batch_id)
        return JSONResponse(content=_build_response(batch_data, batch_id, preview=preview))
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"/batch/{batch_id} error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/batch/{batch_id}/meta", summary="Solo metadatos del batch")
def get_batch_meta(batch_id: int):
    """Rango de bloques del batch sin traer datos. Útil para Airflow pre-check."""
    if not 1 <= batch_id <= NUM_BATCHES:
        raise HTTPException(status_code=422, detail=f"batch_id debe ser 1–{NUM_BATCHES}.")
    try:
        return batcher.get_meta(batch_id)
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/reset", summary="Reinicia el ciclo desde el batch 1")
def reset():
    """Reinicia el puntero. No borra state/file_ids.json ni re-consulta Drive."""
    try:
        batcher.reset()
        state = batcher.initialize()
        return {"message": "Ciclo reiniciado desde batch 1.", "status": state}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/", include_in_schema=False)
def root():
    return {
        "api":        "Rug Pull Batch API v2 (DuckDB)",
        "docs":       "/docs",
        "status":     "/status",
        "next_batch": "/batch/next",
    }