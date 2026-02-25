import logging
from pathlib import Path

from config import DATA_DIR, DRIVE_FILES

logger = logging.getLogger(__name__)


def _check_files():
    """Verifica que los 4 CSVs existan en DATA_DIR antes de usarlos."""
    missing = []
    for filename in DRIVE_FILES.values():
        path = DATA_DIR / filename
        if not path.exists():
            missing.append(str(path))

    if missing:
        raise FileNotFoundError(
            f"Archivos no encontrados en DATA_DIR ({DATA_DIR}):\n"
            + "\n".join(f"  - {p}" for p in missing)
            + "\n\nCopia los 4 CSVs a esa carpeta o ajusta DATA_DIR en el .env"
        )


def get_urls() -> dict[str, str]:
    """
    Retorna las rutas locales de los CSVs como strings.
    DuckDB las consume igual que URLs HTTP.
    """
    _check_files()
    paths = {key: str(DATA_DIR / filename) for key, filename in DRIVE_FILES.items()}
    logger.info(f"Datos locales desde: {DATA_DIR}")
    return paths