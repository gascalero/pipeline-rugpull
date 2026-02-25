set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# ── Colores 
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${GREEN}╔══════════════════════════════════════╗${NC}"
echo -e "${GREEN}║     Rug Pull Batch API — Start       ║${NC}"
echo -e "${GREEN}╚══════════════════════════════════════╝${NC}"

# ── Verificar uv 
if ! command -v uv &> /dev/null; then
    echo -e "${RED}✗ uv no está instalado.${NC}"
    echo "  Instálalo con: curl -LsSf https://astral.sh/uv/install.sh | sh"
    exit 1
fi

# ── Verificar .env
if [ ! -f ".env" ]; then
    echo -e "${RED}✗ No se encontró el archivo .env${NC}"
    echo "  Crea un .env con DATA_DIR y NUM_BATCHES"
    exit 1
fi

# ── Leer DATA_DIR del .env 
DATA_DIR=$(grep -E '^DATA_DIR=' .env | cut -d '=' -f2 | tr -d ' \r\n')
DATA_DIR="${DATA_DIR:-data}"

# Si es ruta relativa, hacerla absoluta desde el directorio del script
if [[ "$DATA_DIR" != /* ]]; then
    DATA_DIR="$SCRIPT_DIR/$DATA_DIR"
fi

# ── Verificar que los CSVs existen
echo -e "\n${YELLOW}Verificando datos...${NC}"
REQUIRED=(
    "eventos_pool_sync_mint_burn.csv"
    "eventos_transfers_tokens.csv"
    "pool_list_complete.csv"
    "token_metadata_complete.csv"
)
MISSING=0
for f in "${REQUIRED[@]}"; do
    if [ -f "${DATA_DIR}/${f}" ]; then
        SIZE=$(du -h "${DATA_DIR}/${f}" | cut -f1)
        echo -e "  ${GREEN}✓${NC} ${f} (${SIZE})"
    else
        echo -e "  ${RED}✗ No encontrado: ${DATA_DIR}/${f}${NC}"
        MISSING=1
    fi
done

if [ "$MISSING" -eq 1 ]; then
    echo -e "\n${RED}Copia los CSVs a la carpeta '${DATA_DIR}/' antes de iniciar.${NC}"
    exit 1
fi

# ── Crear carpetas necesarias
mkdir -p state

# ── Instalar/actualizar dependencias
echo -e "\n${YELLOW}Sincronizando dependencias...${NC}"
uv sync --quiet

# ── Guardar PID
PID_FILE=".api.pid"

# Verificar si ya hay una instancia corriendo
if [ -f "$PID_FILE" ]; then
    OLD_PID=$(cat "$PID_FILE")
    if kill -0 "$OLD_PID" 2>/dev/null; then
        echo -e "\n${YELLOW}⚠ La API ya está corriendo (PID ${OLD_PID}).${NC}"
        echo "  Usa ./stop.sh para detenerla primero."
        exit 1
    else
        rm "$PID_FILE"
    fi
fi

# ── Levantar la API
echo -e "\n${YELLOW}Levantando la API...${NC}"
nohup uv run uvicorn main:app \
    --host 0.0.0.0 \
    --port 8000 \
    --log-level info \
    > api.log 2>&1 &

API_PID=$!
echo $API_PID > "$PID_FILE"

# Esperar a que arranque
echo -n "  Esperando arranque"
for i in {1..15}; do
    sleep 1
    echo -n "."
    if curl -s http://localhost:8000/ > /dev/null 2>&1; then
        echo ""
        break
    fi
done

# Verificar que levantó
if ! curl -s http://localhost:8000/ > /dev/null 2>&1; then
    echo -e "\n${RED}✗ La API no respondió. Revisa api.log${NC}"
    exit 1
fi

echo -e "\n${GREEN}✓ API corriendo (PID ${API_PID})${NC}"
echo -e "  Swagger:    http://localhost:8000/docs"
echo -e "  Status:     http://localhost:8000/status"
echo -e "  Logs:       tail -f api.log"
echo -e "  Detener:    ./stop.sh"

# ── Inicializar batches si es la primera vez 
STATUS=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8000/status)
if [ "$STATUS" -eq 400 ]; then
    echo -e "\n${YELLOW}Primera ejecución — inicializando batches...${NC}"
    INIT=$(curl -s -X POST http://localhost:8000/init)
    echo -e "${GREEN}✓ Batches inicializados${NC}"
else
    CURRENT=$(curl -s http://localhost:8000/status | python3 -c "import json,sys; d=json.load(sys.stdin); print(f\"batch {d['current_batch']} ({d['batches'][d['current_batch']-1]['month']})\")" 2>/dev/null || echo "OK")
    echo -e "\n${GREEN}✓ Estado cargado — próximo: ${CURRENT}${NC}"
fi