"""
simulate_partition_and_archive.py — Módulo para simular particionado lógico y archivado

Incluye lógica para:
- Conectarse a una base de datos PostgreSQL usando SQLAlchemy.
- Agregar columna de partición lógica (`partition_month`) si no existe.
- Poblar dicha columna a partir de un timestamp.
- Crear índice optimizado para consultas por mes.
- Mover datos antiguos a una tabla de archivo (`fact_transactions_archive`).
- Eliminar los registros antiguos de la tabla principal.

Ideal para simular estrategias de particionamiento lógico y mantenimiento histórico
en grandes volúmenes de datos.
"""

import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

def get_engine():
    """
    Crea y retorna un SQLAlchemy Engine configurado con variables de entorno.

    Requiere las variables: DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME
    """
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    db = os.getenv("DB_NAME")
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url)

def simulate_partition_and_archive():
    """
    Ejecuta el proceso completo de simulación de particionado lógico:

    - Añade columna `partition_month` si no existe.
    - La llena a partir del timestamp (`ts`) formateado por mes.
    - Crea un índice en `partition_month` para acelerar filtros.
    - Crea la tabla de archivo si no existe.
    - Mueve registros anteriores a 2025-01 a la tabla de archivo.
    - Borra dichos registros de la tabla principal.
    """
    engine = get_engine()
    with engine.connect() as conn:
        print("🔧 Añadiendo columna partition_month...")
        conn.execute(text("""
            ALTER TABLE fact_transactions
            ADD COLUMN IF NOT EXISTS partition_month TEXT;
        """))

        print("Poblando partition_month desde ts...")
        conn.execute(text("""
            UPDATE fact_transactions
            SET partition_month = TO_CHAR(ts::timestamp, 'YYYY-MM')
            WHERE partition_month IS NULL;
        """))

        print("Creando índice en partition_month...")
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_fact_partition_month
            ON fact_transactions(partition_month);
        """))

        print("Creando tabla de archivo si no existe...")
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS fact_transactions_archive AS
            SELECT *
            FROM fact_transactions
            WHERE FALSE;
        """))

        print("Moviendo datos anteriores a 2025-01 al archivo...")
        conn.execute(text("""
            INSERT INTO fact_transactions_archive
            SELECT *
            FROM fact_transactions
            WHERE partition_month < '2025-01';
        """))

        print("Eliminando registros antiguos de la tabla original...")
        conn.execute(text("""
            DELETE FROM fact_transactions
            WHERE partition_month < '2025-01';
        """))

        print("Partición lógica y archivado completados con éxito.")

if __name__ == "__main__":
    simulate_partition_and_archive()
