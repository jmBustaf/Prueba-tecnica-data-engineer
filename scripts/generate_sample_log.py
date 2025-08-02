"""
generate_sample_log.py — Generador de archivo de logs simulados (JSONL comprimido)

Este script crea un archivo llamado 'sample.log.gz' con 1000 registros de logs simulados en formato JSONL.
Cada registro contiene:
- timestamp: marca de tiempo ISO 8601 en orden descendente
- endpoint: URL de una API
- status_code: código HTTP aleatorio
- response_time: tiempo de respuesta en milisegundos

Este archivo es útil para pruebas de pipelines ETL que procesan logs en streaming.

Ubicación de salida: /data/sample.log.gz
"""

import gzip
import json
from datetime import datetime, timedelta
import random
import os

# Definir ruta de salida y asegurar carpeta
output_path = "data/sample.log.gz"
os.makedirs("data", exist_ok=True)

# Valores posibles para campos simulados
endpoints = ["/api/login", "/api/users", "/api/products", "/api/logout"]
status_codes = [200, 201, 400, 401, 404, 500, 502, 503]
base_time = datetime.utcnow()

# Abrir archivo gzip para escritura de texto
with gzip.open(output_path, "wt", encoding="utf-8") as f:
    for i in range(1000):
        # Crear log simulado con timestamp descendente
        log = {
            "timestamp": (base_time - timedelta(minutes=i)).isoformat() + "Z",
            "endpoint": random.choice(endpoints),
            "status_code": random.choice(status_codes),
            "response_time": random.randint(50, 1200)
        }
        # Escribir como línea JSON
        f.write(json.dumps(log) + "\n")

print("✅ Archivo sample.log.gz generado en /data")
