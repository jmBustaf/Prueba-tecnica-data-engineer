"""
transform_log.py — Módulo de transformación de registros de log
Incluye lógica para:
- Filtrar registros con errores del servidor (status_code >= 500).
- Limpiar campos requeridos y convertir fechas.
- Agregar métricas de cantidad y tiempo promedio de respuesta por hora y endpoint.
"""

import pandas as pd
from datetime import datetime
from typing import List, Dict
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s — %(levelname)s — %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def transform_records(records: List[Dict]) -> pd.DataFrame:
    """
    Transforma una lista de registros JSON en un DataFrame limpio,
    filtrando por status_code >= 500 y agregando métricas por hora + endpoint.

    Retorna:
    - Un DataFrame con columnas: hour, endpoint, total_requests, avg_response_time
    """
    if not records:
        logger.warning("No se recibieron registros para transformar.")
        return pd.DataFrame()

    df = pd.DataFrame(records)

    try:
        # Asegurar tipos y campos requeridos
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df = df[df["status_code"] >= 500]
        df = df.dropna(subset=["timestamp", "endpoint", "status_code"])

        # Crear columna `hour` redondeada a la hora
        df["hour"] = df["timestamp"].dt.floor("H")

        # Agregación: métricas por hora y endpoint
        metrics = df.groupby(["hour", "endpoint"]).agg(
            total_requests=("status_code", "count"),
            avg_response_time=("response_time", "mean")
        ).reset_index()

        return metrics

    except Exception as e:
        logger.error(f"Error transformando registros: {str(e)}")
        raise
