"""
 logger.py — Configuración de logging estandarizado
Devuelve un logger con formato consistente y nivel INFO.
"""

import logging

def get_logger(name: str):
    """
    Crea y retorna un logger con formato estándar:
    [timestamp — nombre — nivel — mensaje]
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s — %(name)s — %(levelname)s — %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger
