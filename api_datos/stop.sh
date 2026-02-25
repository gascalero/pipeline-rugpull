set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

RESET=0
if [[ "$1" == "--reset" ]]; then
    RESET=1
fi

if [ "$RESET" -eq 1 ]; then
    echo -e "${RED}╔══════════════════════════════════════╗${NC}"
    echo -e "${RED}║   Rug Pull Batch API — Stop + Reset  ║${NC}"
    echo -e "${RED}╚══════════════════════════════════════╝${NC}"
else
    echo -e "${YELLOW}╔══════════════════════════════════════╗${NC}"
    echo -e "${YELLOW}║     Rug Pull Batch API — Stop        ║${NC}"
    echo -e "${YELLOW}╚══════════════════════════════════════╝${NC}"
fi

PID_FILE=".api.pid"

# ── Detener el proceso
if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if kill -0 "$PID" 2>/dev/null; then
        echo -e "\nDeteniendo API (PID ${PID})..."
        kill "$PID"

        for i in {1..10}; do
            sleep 1
            if ! kill -0 "$PID" 2>/dev/null; then
                break
            fi
        done

        if kill -0 "$PID" 2>/dev/null; then
            echo -e "${YELLOW}  Forzando cierre...${NC}"
            kill -9 "$PID" 2>/dev/null || true
        fi

        rm -f "$PID_FILE"
        echo -e "${GREEN}✓ API detenida.${NC}"
    else
        echo -e "${YELLOW}⚠ El proceso (PID ${PID}) ya no existe.${NC}"
        rm -f "$PID_FILE"
    fi
else
    echo -e "${YELLOW}No se encontró .api.pid — buscando proceso por nombre...${NC}"
    PIDS=$(pgrep -f "uvicorn main:app" 2>/dev/null || true)

    if [ -n "$PIDS" ]; then
        echo "  Procesos encontrados: $PIDS"
        kill $PIDS 2>/dev/null || true
        sleep 2
        echo -e "${GREEN}✓ API detenida.${NC}"
    else
        echo -e "${RED}✗ No se encontró ninguna instancia de la API corriendo.${NC}"
        if [ "$RESET" -eq 0 ]; then
            exit 1
        fi
    fi
fi

# ── Reset del estado
if [ "$RESET" -eq 1 ]; then
    echo ""
    if [ -f "state/batch_state.json" ]; then
        rm "state/batch_state.json"
        echo -e "${RED}✓ Estado borrado — próximo start iniciará desde el batch 1.${NC}"
    else
        echo -e "${YELLOW}  No había estado que borrar.${NC}"
    fi
fi

echo -e "  Logs guardados en: api.log"