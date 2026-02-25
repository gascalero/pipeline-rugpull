import logging
import threading
import duckdb

logger = logging.getLogger(__name__)

# DuckDB en memoria — una conexión por hilo (thread-safe)
_local = threading.local()


def get_con() -> duckdb.DuckDBPyConnection:
    """
    Retorna una conexión DuckDB en memoria.
    Se crea una por hilo y se reutiliza.
    """
    if not hasattr(_local, "con"):
        con = duckdb.connect(database=":memory:")
        _local.con = con
        logger.info("Conexión DuckDB inicializada.")
    return _local.con


# ── Queries de batch

def query_events(url: str, block_start: int, block_end: int) -> list[dict]:
    """Lee eventos filtrados por rango de bloques."""
    sql = f"""
        SELECT pair_address, event_type, block_number, transaction_hash,
               amount0_or_reserve0_hex, amount1_or_reserve1_hex
        FROM read_csv_auto('{url}', ignore_errors=true)
        WHERE block_number BETWEEN {block_start} AND {block_end}
    """
    try:
        return get_con().execute(sql).df().to_dict(orient="records")
    except Exception:
        sql_fallback = f"""
            SELECT * FROM read_csv_auto('{url}', ignore_errors=true)
            WHERE block_number BETWEEN {block_start} AND {block_end}
        """
        return get_con().execute(sql_fallback).df().to_dict(orient="records")


def query_transfers(url: str, block_start: int, block_end: int) -> list[dict]:
    """
    Lee transfers filtradas por rango de bloques.
    Solo columnas esenciales para reducir payload y tiempo de lectura.
    """
    sql = f"""
        SELECT token_address, from_address, to_address,
               block_number, transaction_hash, value_hex
        FROM read_csv_auto('{url}', ignore_errors=true)
        WHERE block_number BETWEEN {block_start} AND {block_end}
    """
    try:
        return get_con().execute(sql).df().to_dict(orient="records")
    except Exception:
        logger.warning("Transfers: fallback a SELECT *")
        try:
            sql_fallback = f"""
                SELECT * FROM read_csv_auto('{url}', ignore_errors=true)
                WHERE block_number BETWEEN {block_start} AND {block_end}
            """
            return get_con().execute(sql_fallback).df().to_dict(orient="records")
        except Exception:
            logger.warning("Transfers sin block_number — se omite.")
            return []


def query_pools(url: str, pair_addresses: list[str]) -> list[dict]:
    """Lee pools filtrados por las direcciones activas en el batch."""
    if not pair_addresses:
        return []
    addr_list = ", ".join(f"'{a}'" for a in pair_addresses)
    sql = f"""
        SELECT *
        FROM read_csv_auto('{url}', ignore_errors=true)
        WHERE pair_address IN ({addr_list})
    """
    return get_con().execute(sql).df().to_dict(orient="records")


def query_metadata(url: str, token_addresses: list[str]) -> list[dict]:
    """Lee metadata de los tokens activos en el batch."""
    if not token_addresses:
        return []
    addr_list = ", ".join(f"'{a}'" for a in token_addresses)
    # token_address puede llamarse distinto — intentamos ambas columnas
    sql = f"""
        SELECT *
        FROM read_csv_auto('{url}', ignore_errors=true)
        WHERE COALESCE(token_address, address, token) IN ({addr_list})
    """
    try:
        return get_con().execute(sql).df().to_dict(orient="records")
    except Exception:
        logger.warning("Fallback en metadata: trayendo todas las filas.")
        return get_con().execute(
            f"SELECT * FROM read_csv_auto('{url}', ignore_errors=true)"
        ).df().to_dict(orient="records")


def get_block_range(url: str) -> tuple[int, int]:
    """
    Obtiene el rango global de bloques del CSV de eventos.
    Detecta automáticamente la columna de bloques (block_number, block, etc.).
    """
    cols = get_con().execute(
        f"SELECT * FROM read_csv_auto('{url}', ignore_errors=true) LIMIT 0"
    ).description
    col_names = [c[0].lower() for c in cols]
    logger.info(f"Columnas detectadas en events: {col_names}")

    block_col = next(
        (c for c in col_names if c == "block_number"),
        next(
            (c for c in col_names if "block" in c and "creation" not in c),
            None
        )
    )
    if not block_col:
        raise ValueError(
            f"No se encontró columna de block_number en el CSV de eventos.\n"
            f"Columnas disponibles: {col_names}\n"
            f"Verifica que FILE_ID_EVENTS apunte al archivo correcto: "
            f"eventos_pool_sync_mint_burn.csv"
        )
    logger.info(f"Usando columna de bloque: '{block_col}'")

    sql = f"""
        SELECT
            MIN("{block_col}") AS block_min,
            MAX("{block_col}") AS block_max
        FROM read_csv_auto('{url}', ignore_errors=true)
    """
    row = get_con().execute(sql).fetchone()
    return int(row[0]), int(row[1])


def get_active_pairs(url: str, block_start: int, block_end: int) -> list[str]:
    """Retorna las pair_addresses con al menos 1 evento en el rango."""
    sql = f"""
        SELECT DISTINCT pair_address
        FROM read_csv_auto('{url}', ignore_errors=true)
        WHERE block_number BETWEEN {block_start} AND {block_end}
    """
    rows = get_con().execute(sql).fetchall()
    return [r[0] for r in rows]

def get_active_tokens(pools_url: str, pair_addresses: list[str]) -> list[str]:
    """
    Retorna los token_address no-wETH de los pools activos.
    El CSV de pools tiene token0 y token1 — se excluye wETH de ambos.
    """
    if not pair_addresses:
        return []
    weth = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    addr_list = ", ".join(f"'{a}'" for a in pair_addresses)
    sql = f"""
        SELECT DISTINCT
            CASE
                WHEN LOWER(token0) = '{weth}' THEN token1
                ELSE token0
            END AS token_address
        FROM read_csv_auto('{pools_url}', ignore_errors=true)
        WHERE pair_address IN ({addr_list})
          AND token0 IS NOT NULL
          AND token1 IS NOT NULL
    """
    try:
        rows = get_con().execute(sql).fetchall()
        tokens = [r[0] for r in rows if r[0]]
        logger.info(f"Tokens activos encontrados: {len(tokens)}")
        return tokens
    except Exception as e:
        logger.warning(f"get_active_tokens falló: {e}")
        return []