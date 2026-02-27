#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=============================================="
echo "  Gestion del stack MLOps"
echo "=============================================="
echo "  1) Detener servicios (conserva datos y volumenes)"
echo "  2) Eliminar servicios y volumenes (conserva cache de Docker)"
echo "  3) Eliminar servicios, volumenes e imagenes (limpieza completa)"
echo "  0) Cancelar"
echo "=============================================="
read -p "Selecciona una opcion: " opcion

case $opcion in
  1)
    echo "Deteniendo servicios..."
    docker compose stop
    echo ""
    echo "Servicios detenidos. Datos y volumenes conservados."
    echo "Para reiniciar ejecuta: docker compose start"
    ;;
  2)
    read -p "ADVERTENCIA: Se eliminaran contenedores y volumenes. Continuar? (s/N): " confirm
    if [[ ! "$confirm" =~ ^[sS]$ ]]; then echo "Operacion cancelada."; exit 0; fi
    echo "Deteniendo servicios y eliminando volumenes..."
    docker compose down --volumes --remove-orphans
    if [ -d "./minio" ]; then
      rm -rf ./minio/*
      echo "OK - Datos de MinIO eliminados"
    fi
    echo "Stack eliminado. Para redesplegar ejecuta: ./deploy.sh"
    ;;
  3)
    read -p "ADVERTENCIA: Se eliminaran contenedores, volumenes e imagenes. Continuar? (s/N): " confirm
    if [[ ! "$confirm" =~ ^[sS]$ ]]; then echo "Operacion cancelada."; exit 0; fi
    echo "Deteniendo servicios y eliminando volumenes..."
    docker compose down --volumes --remove-orphans
    echo "Eliminando imagenes del proyecto..."
    docker compose down --rmi all 2>/dev/null || true
    if [ -d "./minio" ]; then
      rm -rf ./minio/*
      echo "OK - Datos de MinIO eliminados"
    fi
    echo "Limpiando recursos Docker sin uso..."
    docker system prune -f
    echo "Limpieza completa finalizada. Para redesplegar ejecuta: ./deploy.sh"
    ;;
  0)
    echo "Operacion cancelada."
    exit 0
    ;;
  *)
    echo "Opcion invalida."
    exit 1
    ;;
esac

echo "=============================================="