"""
setup_etl_log_structure.py — Script de inicialización de estructura para ETL de logs

Este script crea la estructura base de carpetas y archivos necesarios para desarrollar un pipeline ETL que procese archivos de log comprimidos.

Carpetas creadas:
- etl_log/: módulos de extracción, transformación y carga
- data/: archivos de entrada
- outputs/: archivos de salida (ej. Parquet)
- scripts/: ejecutables principales del pipeline

Archivos creados:
- Módulos vacíos con encabezados para cada etapa del ETL
"""

import os

# Carpetas requeridas para el pipeline
folders = [
    "etl_log",
    "data",
    "outputs",
    "scripts"
]

# Archivos base con contenido de encabezado o docstring inicial
files = {
    "etl_log/__init__.py": "",
    "etl_log/extract_log.py": "# Lectura gzip línea por línea\n",
    "etl_log/transform_log.py": "# Limpieza, parseo y agrupación\n",
    "etl_log/export_log.py": "# Exportar a Parquet\n",
    "etl_log/profiler.py": "# Medición de tiempos y memoria\n",
    "scripts/run_log_etl.py": "# Pipeline principal de log ETL\n",
}

# Crear carpetas si no existen
for folder in folders:
    os.makedirs(folder, exist_ok=True)

# Crear archivos con contenido base
for path, content in files.items():
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)

print("✅ Estructura ETL log creada exitosamente.")
