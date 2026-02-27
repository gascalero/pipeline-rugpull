#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Cargar variables del .env
if [ ! -f .env ]; then
  echo "ERROR: No se encontro el archivo .env en $SCRIPT_DIR"
  exit 1
fi

export $(grep -v '^#' .env | xargs)

echo "Iniciando despliegue del stack MLOps..."
echo "=============================================="

# Construir y levantar todos los servicios
echo "Construyendo imagenes y levantando servicios..."
docker compose up -d --build

# Esperar a que MinIO este listo
echo "Esperando a que MinIO este disponible..."
until docker exec Minio mc alias set myminio http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null 2>&1; do
  echo "  MinIO aun no esta listo, reintentando..."
  sleep 5
done
echo "OK - MinIO esta disponible"

# Verificar si el bucket ya existe, si no, crearlo
echo "Verificando bucket '$MINIO_BUCKET'..."
if docker exec Minio mc ls myminio/"$MINIO_BUCKET" >/dev/null 2>&1; then
  echo "OK - El bucket '$MINIO_BUCKET' ya existe"
else
  echo "Bucket no encontrado, creando '$MINIO_BUCKET'..."
  docker exec Minio mc mb myminio/"$MINIO_BUCKET"
  echo "OK - Bucket '$MINIO_BUCKET' creado exitosamente"
fi

# Esperar a que MLflow este listo
echo "Esperando a que MLflow este disponible..."
until curl -sf http://localhost:"$MLFLOW_PORT"/ >/dev/null 2>&1; do
  echo "  MLflow aun no esta listo, reintentando..."
  sleep 5
done
echo "OK - MLflow esta disponible"

echo ""
echo "Despliegue completado exitosamente"
echo "=============================================="
echo "  MLflow:         http://localhost:${MLFLOW_PORT}"
echo "  MinIO Console:  http://localhost:${MINIO_CONSOLE_PORT}"
echo "  Airflow:        http://localhost:8080"
echo "  JupyterLab:     http://localhost:${JUPYTER_PORT}"
echo "  PgAdmin:        http://localhost:${PGADMIN_PORT}"
echo "=============================================="