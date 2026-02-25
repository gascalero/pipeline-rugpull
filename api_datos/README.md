# Rug Pull Batch API

API de datos para el proyecto de detección de rug pulls en pools Uniswap V2 sobre la red Ethereum. Su única responsabilidad es servir los datos históricos en batches mensuales ordenados cronológicamente para alimentar un proceso de entrenamiento de modelos de machine learning.

---

## Contexto y propósito

### ¿Qué es un rug pull?

Un rug pull es un tipo de fraude en DeFi donde los creadores de un token retiran repentinamente toda la liquidez de un pool, dejando a los demás inversores con tokens sin valor. Los patrones de este comportamiento se manifiestan en los eventos on-chain del pool y en los movimientos del token, particularmente en las primeras horas y días de vida del pool.

### ¿Qué hace esta API?

Sirve datos históricos de pools Uniswap V2 divididos en **12 batches mensuales** consecutivos. Cada batch representa un mes calendario y contiene los 4 datasets filtrados a ese período:

- Eventos del pool (SYNC, MINT, BURN)
- Transfers del token
- Información del pool
- Metadata del token (incluye el label `is_rugpull`)

### ¿Qué NO hace esta API?

- No aplica heurísticas ni feature engineering
- No hace predicciones
- No entrena modelos
- No sirve datos para inferencia en producción

Esas responsabilidades pertenecen a otras capas del sistema.

---

## Dataset

Los datos cubren pools de Uniswap V2 entre **junio 2020 y mayo 2021**, período que incluye el auge inicial del ecosistema DeFi y una alta concentración de eventos de rug pull documentados.

### Archivos

| Archivo | Descripción | Aprox. |
|---|---|---|
| `eventos_pool_sync_mint_burn.csv` | Eventos SYNC, MINT y BURN de cada pool | 1.8M filas |
| `eventos_transfers_tokens.csv` | Transferencias ERC-20 del token entre wallets | 8.7M filas / 2 GB |
| `pool_list_complete.csv` | Directorio de pools con sus tokens y fecha de creación | 998 filas |
| `token_metadata_complete.csv` | Metadata del token: creador, bloque de creación, `is_rugpull` | 998 filas |

### Criterios de inclusión del dataset

Un pool está incluido si cumple:
1. Uno de los dos tokens del par es **wETH** (`0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2`) — la moneda de referencia en Uniswap V2
2. Tiene al menos **5 eventos SYNC** — mínimo de actividad para que los patrones sean detectables

### Modelo de datos: las dos llaves

**`pair_address`** — dirección del contrato del pool. Identifica de forma única todos los eventos SYNC/MINT/BURN asociados a ese par de tokens.

**`token_address`** — dirección del token no-wETH del par. Identifica los transfers ERC-20 entre wallets externas al pool, que revelan patrones de concentración y distribución del token.

### Eventos del pool

| Evento | Qué registra | Relevancia para rug pull |
|---|---|---|
| `SYNC` | Estado de las reservas del pool en cada bloque | Permite reconstruir el precio y la liquidez a lo largo del tiempo |
| `MINT` | Depósito de liquidez por un proveedor de liquidez (LP) | Muestra quién añade liquidez y cuándo |
| `BURN` | Retiro de liquidez por un LP | Un BURN masivo y súbito es la señal principal de rug pull |

---

## Batches mensuales

### ¿Por qué batches mensuales?

Imita el flujo real de un proceso MLOps donde la información llega periódicamente. Dividir por mes calendario permite:

- Entrenar el modelo con un período de tiempo a la vez
- Comparar distribuciones de features entre meses para detectar data drift
- Reproducir experimentos con períodos específicos

### ¿Cómo se calculan los límites de cada batch?

Los datos están indexados por `block_number` de Ethereum, no por timestamp. La conversión usa la referencia:

```
Bloque 10,000,000 = 11 junio 2020 (UTC)
Velocidad promedio = 13.2 segundos por bloque
```

A partir de ahí se calculan los bloques de inicio y fin de cada mes calendario.

### Mapa de batches

```
Batch  1: 2020-06 | bloques 10,091,132 → 10,130,908
Batch  2: 2020-07 | bloques 10,130,909 → 10,333,817
Batch  3: 2020-08 | bloques 10,333,818 → 10,536,726
Batch  4: 2020-09 | bloques 10,536,727 → 10,733,089
Batch  5: 2020-10 | bloques 10,733,090 → 10,935,999
Batch  6: 2020-11 | bloques 10,936,000 → 11,132,362
Batch  7: 2020-12 | bloques 11,132,363 → 11,335,271
Batch  8: 2021-01 | bloques 11,335,272 → 11,538,180
Batch  9: 2021-02 | bloques 11,538,181 → 11,721,453
Batch 10: 2021-03 | bloques 11,721,454 → 11,924,362
Batch 11: 2021-04 | bloques 11,924,363 → 12,120,726
Batch 12: 2021-05 | bloques 12,120,727 → 12,323,635
```

### Ciclo infinito

Una vez que se sirve el batch 12, el puntero vuelve automáticamente al batch 1. Esto permite repetir el ciclo completo de entrenamiento sin intervención manual.

```
batch 1 → batch 2 → ... → batch 12 → batch 1 → batch 2 → ...
```

El estado del ciclo (cuál es el próximo batch) se persiste en `state/batch_state.json`. Si la API se reinicia sin reset, el ciclo continúa desde donde quedó.

---

## Arquitectura técnica

### Stack

- **FastAPI** — framework de la API
- **DuckDB** — motor de consultas SQL en memoria
- **uv** — gestión del entorno Python

### Por qué DuckDB

Los CSVs se leen directamente desde disco con SQL. DuckDB filtra por `block_number` sin cargar el archivo completo en RAM, lo que permite consultar 8.7M filas de transfers en segundos.

```
Consumidor → GET /batch/next
                    │
               DuckDB (en memoria)
                    │
          SQL WHERE block_number BETWEEN x AND y
                    │
          ┌─────────┼──────────┬──────────────┐
          │         │          │              │
     eventos.csv  pools.csv  metadata.csv  transfers.csv
     (disco local)
```

### Estructura del proyecto

```
api_datos/
├── main.py               # FastAPI + endpoints
├── config.py             # Variables del .env
├── pyproject.toml        # Dependencias (uv)
├── .env                  # Configuración local
├── start.sh              # Levanta la API
├── stop.sh               # Detiene la API
│
├── data/                 # CSVs (no subir a git — son pesados)
│   ├── eventos_pool_sync_mint_burn.csv
│   ├── eventos_transfers_tokens.csv
│   ├── pool_list_complete.csv
│   └── token_metadata_complete.csv
│
├── services/
│   ├── drive.py          # Resuelve rutas de los CSVs
│   ├── db.py             # Queries DuckDB
│   └── batcher.py        # Ciclo mensual + conversión bloque → mes
│
└── state/                # Auto-generado
    └── batch_state.json  # Puntero del ciclo actual
```

---

## Setup

### Requisitos

- Python 3.11–3.13
- [`uv`](https://docs.astral.sh/uv/) instalado
- Los 4 CSVs en la carpeta `data/`

### Instalación

```bash
cd api_datos
uv venv
uv sync
```

### .env

```bash
# Carpeta donde están los 4 CSVs
DATA_DIR=data

# Número de batches mensuales
NUM_BATCHES=12
```

---

## Operación

### Levantar

```bash
./start.sh
```

- Verifica que los CSVs existan en `data/`
- Sincroniza dependencias
- Levanta la API en background (puerto 8000)
- Inicializa los 12 batches automáticamente si es la primera vez
- Si hay estado previo, reanuda desde el batch donde quedó

### Detener (conservando el estado)

```bash
./stop.sh
```

La próxima vez que se levante, el ciclo continúa desde el batch donde se detuvo.

### Detener y reiniciar el ciclo desde cero

```bash
./stop.sh --reset
```

Borra `state/batch_state.json`. La próxima vez que se levante, el ciclo inicia desde el batch 1.

---

## Endpoints

### `POST /init`

Inicializa los 12 batches calculando los rangos de bloques de cada mes. Solo necesita ejecutarse una vez — si el estado ya existe, no hace nada.

```bash
curl -X POST http://localhost:8000/init
```

### `GET /status`

Retorna el estado completo del ciclo: batch actual, mapa de los 12 batches con sus rangos de bloques y fechas.

```bash
curl http://localhost:8000/status
```

### `GET /batch/next`

Retorna los datos del siguiente batch en el ciclo y avanza el puntero. Es el endpoint principal de consumo.

```bash
# Datos completos
curl http://localhost:8000/batch/next

# Preview: solo 10 filas por dataset (para explorar sin crashear el browser)
curl "http://localhost:8000/batch/next?preview=true"
```

**Parámetros:**
- `preview` (bool, default `false`) — si es `true`, retorna solo las primeras 10 filas de cada dataset

**Respuesta:**
```json
{
  "batch_id": 3,
  "batch_total": 12,
  "month": "2020-08",
  "block_start": 10333818,
  "block_end": 10536726,
  "served_at": "2026-02-24T10:00:00Z",
  "next_batch_id": 4,
  "preview": false,
  "summary": {
    "month": "2020-08",
    "n_pools": 45,
    "n_events": 98000,
    "n_events_by_type": {
      "SYNC": 71000,
      "MINT": 14000,
      "BURN": 13000
    },
    "n_metadata_rows": 45,
    "n_transfers": 312000
  },
  "data": {
    "pools":     [...],
    "events":    [...],
    "metadata":  [...],
    "transfers": [...]
  }
}
```

### `GET /batch/{batch_id}`

Retorna los datos de un batch específico **sin mover el puntero**. Útil para re-pedir un batch si el proceso que lo consumió falló.

```bash
curl "http://localhost:8000/batch/3"
curl "http://localhost:8000/batch/3?preview=true"
```

### `GET /batch/{batch_id}/meta`

Retorna solo el rango de bloques y el mes del batch, sin datos. Respuesta instantánea — no consulta los CSVs.

```bash
curl http://localhost:8000/batch/3/meta
```

```json
{
  "id": 3,
  "month": "2020-08",
  "block_start": 10333818,
  "block_end": 10536726
}
```

### `POST /reset`

Reinicia el puntero al batch 1 y re-inicializa el estado. Equivalente a `./stop.sh --reset && ./start.sh` pero sin reiniciar el proceso.

```bash
curl -X POST http://localhost:8000/reset
```

---

## Inspeccionar datos desde la terminal

El Swagger (`/docs`) no puede renderizar batches completos — algunos tienen más de 400k filas. Usar la terminal:

**Ver el resumen del siguiente batch sin consumirlo:**
```bash
curl -s "http://localhost:8000/batch/next?preview=true" | python3 -c "
import json, sys
d = json.load(sys.stdin)
print('batch :', d['batch_id'], '/', d['batch_total'])
print('mes   :', d['month'])
print('bloques:', d['block_start'], '→', d['block_end'])
print(json.dumps(d['summary'], indent=2))
"
```

**Guardar un batch completo a disco:**
```bash
curl -s http://localhost:8000/batch/next > batch_agosto_2020.json
```

**Ver metadatos de todos los batches:**
```bash
for i in $(seq 1 12); do
  curl -s http://localhost:8000/batch/$i/meta
  echo ""
done
```

---

## Gestión del entorno

```bash
uv sync                  # instalar/actualizar dependencias
uv add <paquete>         # agregar dependencia nueva
rm -rf .venv             # eliminar entorno
uv venv && uv sync       # recrear desde cero
```

El `.venv/` vive dentro de la carpeta del proyecto. Borrarlo elimina todo sin dejar rastro en el sistema.

---

## .gitignore recomendado

```
.venv/
data/
state/
.env
api.log
.api.pid
__pycache__/
*.pyc
```

Los CSVs no deben subirse al repositorio por su tamaño. El `.env` no debe subirse por seguridad. El `state/` es generado en tiempo de ejecución.