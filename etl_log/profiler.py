"""
profiler.py — Módulo para perfilamiento de funciones
Incluye lógica para:
- Medir el tiempo de ejecución y uso pico de memoria de cualquier función.
- Registrar estos valores con logging estructurado.
"""

import time
import tracemalloc
import logging
from typing import Callable, Tuple, Any

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s — %(levelname)s — %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def profile_function(func: Callable, *args, **kwargs) -> Tuple[Any, float, float]:
    """
    Ejecuta una función y mide su tiempo y memoria usada.

    Parámetros:
    - func: Función a ejecutar.
    - *args, **kwargs: Argumentos de la función.

    Retorna:
    - result: Resultado devuelto por la función original.
    - elapsed_time: Tiempo total de ejecución en segundos.
    - peak_memory: Memoria pico utilizada durante la ejecución (en MB).
    """
    tracemalloc.start()
    start_time = time.perf_counter()

    result = func(*args, **kwargs)

    end_time = time.perf_counter()
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    elapsed_time = end_time - start_time
    peak_memory = peak / 1024 / 1024  # Convertir a MB

    logger.info(f"Tiempo de ejecución: {elapsed_time:.2f} segundos")
    logger.info(f"Memoria pico usada: {peak_memory:.2f} MB")

    return result, elapsed_time, peak_memory
