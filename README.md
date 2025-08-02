## Requisitos previos

### Crear archivo .env con las variables necesarias:

.env

DB_USER=usuario
DB_PASSWORD=clave
DB_HOST=localhost
DB_PORT=5432
DB_NAME=nombre_base_datos


### Instalar dependencias:

pip install -r requirements.txt



## Ejercicio 1: Orquestación local (ETL desde CSV)

Este módulo implementa un pipeline ETL end-to-end en entorno local, utilizando el archivo sample_transactions.csv (1 millón de registros simulados). El flujo incluye sensores de archivo, transformación por lotes y carga en PostgreSQL, todo de forma modular y reutilizable.



### Objetivo cumplido

✔️ Ejecutar un workflow completo local
✔️ Leer datos por chunks para eficiencia de memoria
✔️ Transformar columnas de forma robusta
✔️ Cargar en base de datos relacional
✔️ Validar disponibilidad del archivo antes de procesar
✔️ Manejo de errores con logs
✔️ Estructura modular con separación por etapa
✔️ Comentarios y documentación inline
✔️ Listo para extensión con testing y CI/CD



### Descripción de las tareas

#### Crear un DAG o flujo equivalente local

##### Se implementó un pipeline modular y secuencial desde main.py, actuando como orquestador local sin depender de frameworks como Airflow.

#### Descargar, transformar y cargar datos

##### La transformación se realiza con Pandas (transform_csv.py) y la carga en PostgreSQL con SQLAlchemy (load_to_db.py). El archivo ya se encuentra localmente, pero el pipeline incluye sensor para verificar su existencia.

#### Validar que la tabla no esté vacía

##### Al final del pipeline se usa validate_table_not_empty() para verificar que se hayan insertado registros correctamente.

#### Reintentos y sensores

##### Se incluye wait_for_file() como sensor de archivo con espera activa, tamaño mínimo y timeout configurable.

#### Modularización del pipeline

##### Cada etapa del proceso (extracción, transformación, carga, logging, conexión DB) está separada en módulos independientes y reutilizables.

#### Tests unitarios/integración (pendiente)

##### La estructura está lista para agregar pruebas, aunque no se incluyen en esta entrega.

#### Extras implementados

##### Lectura por chunks (read_csv_chunks) para uso eficiente de memoria

##### Logging uniforme en todas las etapas

##### Preparado para extensión a .gz u otros formatos en ejercicios posteriores



### Estructura del módulo

| Archivo                 | Función                                      |
|-------------------------|----------------------------------------------|
| `etl/extract_csv.py`    | Sensor y lectura por chunks del CSV          |
| `etl/transform_csv.py`  | Limpieza y normalización de columnas         |
| `etl/load_to_db.py`     | Inserción en base de datos PostgreSQL        |
| `etl/pipeline_runner.py`| Orquestador principal del pipeline           |
| `etl/db.py`             | Configuración del engine SQLAlchemy          |
| `etl/logger.py`         | Logger con formato estándar                  |
| `main.py`               | Script principal para ejecutar el ETL local  |



### Flujo del pipeline

#### Sensor de archivo

##### Espera hasta que el archivo exista y tenga al menos 10 KB.

##### Tiempo máximo de espera configurable (default: 30 segundos).

#### Extracción

##### Lee el CSV por chunks de 100.000 filas con pandas.read_csv(chunksize=...).

#### Transformación

##### Convierte nombres de columnas a minúscula.

##### Transforma transaction_date a datetime con control de errores.

#### Carga

##### Inserta cada chunk en PostgreSQL usando SQLAlchemy (to_sql con method="multi").

##### El proceso es incremental (if_exists="append").

#### Manejo de errores y logging

##### Cada etapa está envuelta en bloques try/except.

##### Logging uniforme configurado en etl/logger.py.



### Ejecución del pipeline

#### Opción A — Modo directo (rápido para pruebas)

python main.py

#### Opción B — Modo CLI con argumentos

python -m etl.pipeline_runner data/sample_transactions.csv transactions



### Validaciones y observaciones

#### El pipeline no se ejecuta si el archivo no existe o es menor a 10 KB.

#### Logs detallados permiten rastrear errores y estado por etapa.

#### La validación post-carga (tabla no vacía) puede añadirse fácilmente en load_to_db.py.

#### Se puede adaptar para SQLite con un cambio en el engine.



### Decisiones técnicas

#### Se eligió un enfoque basado en funciones puras (en lugar de clases) por simplicidad y claridad.

#### Se evitó el uso de frameworks (Airflow, Prefect) para mantener la ejecución simple y local.

#### La conexión a base de datos se centraliza para facilitar pruebas y despliegue en diferentes entornos.

#### La estructura modular permite reutilizar cada componente por separado.





## Ejercicio 2: SQL y análisis

Este módulo contiene un conjunto de consultas SQL analíticas y scripts auxiliares que permiten ejecutar análisis sobre los datos previamente cargados desde sample_transactions.csv a PostgreSQL.

Se abordan tareas clave de agregación, detección de anomalías, validación de datos y optimización de rendimiento mediante índices, triggers y estrategias de partición.



### Objetivo cumplido

✔️ Crear vista agregada por día y estado
✔️ Detectar usuarios con múltiples fallos en los últimos 7 días
✔️ Identificar días anómalos con transacciones excesivas
✔️ Agregar índices y triggers para validación y rendimiento
✔️ Implementar estrategia de partición lógica
✔️ Automatizar ejecución de scripts SQL
✔️ Documentar cada etapa de forma clara y reutilizable



### Descripción de las tareas

#### Crear vista/tabla resumen por día y estado

##### La vista daily_summary agrupa las transacciones por fecha (ts) y estado (status), mostrando el total por combinación.

####  Query para detectar usuarios con >3 transacciones fallidas

##### Consulta que retorna user_id con más de 3 fallos (status = 'failed') en los últimos 7 días.

#### Detección de anomalías

##### Se utiliza AVG y STDDEV para calcular la media y desviación estándar del número de transacciones por día, y detectar si el último día supera 2 desviaciones estándar.

#### Crear índices y triggers

##### Índices en user_id, order_id para optimizar consultas.

##### Trigger validate_amount_range impide insertar montos negativos.

#### Partición lógica

##### La tabla transactions se redefine como particionada por rangos de fechas (transaction_date) con ejemplos mensuales.

#### Modularización en archivos SQL

##### Cada consulta o grupo de lógica está separada por archivo: vistas, queries, índices, triggers y partición.



### Estructura del módulo

| Archivo                              | Función                                                               |
| ------------------------------------ | --------------------------------------------------------------------- |
| `sql/views/daily_summary.sql`        | Vista con resumen de transacciones por día y estado                   |
| `sql/queries/failed_users_last7.sql` | Consulta de usuarios con más de 3 fallos en 7 días                    |
| `sql/queries/detect_anomalies.sql`   | Detección de anomalías estadísticas (2 desviaciones estándar)         |
| `sql/indices_and_triggers.sql`       | Creación de índices y trigger para validar montos negativos           |
| `sql/partition_strategy.sql`         | Definición de tabla particionada por rango de fechas                  |
| `run_analysis.py`                    | Script principal para ejecutar todos los archivos SQL automáticamente |



### Flujo del pipeline

#### Creación de vista

##### daily_summary agrupa por día y estado para facilitar análisis de volumen diario.

#### Detección de fallos

##### Consulta escanea los últimos 7 días buscando usuarios con múltiples fallos.

#### Anomalías

##### Identifica si el último día tiene un comportamiento anómalo usando estadística básica.

#### Optimización

##### Se crean índices relevantes y un trigger para validar el campo amount.

#### Partición

##### Se define tabla particionada y se crean particiones por mes.

#### Automatización

##### El script run_analysis.py ejecuta todos los archivos SQL en orden.



### Ejecución del pipeline

python run_analysis.py



### Validaciones y observaciones

#### Las consultas dependen de que la tabla transactions ya esté poblada correctamente desde el ETL del Ejercicio 1.

#### El script run_analysis.py no valida si los archivos SQL ya fueron ejecutados previamente. Puede ejecutarse múltiples veces sin errores si se usan CREATE OR REPLACE.

#### El trigger validate_amount_range evitará cualquier inserción o actualización con montos negativos. Esto puede lanzar errores si no se controlan los datos antes desde la aplicación.

#### Los índices creados mejoran el rendimiento de lectura, pero podrían afectar la velocidad de escritura si se insertan muchos datos a gran escala.

#### La partición por mes es útil para cargas masivas históricas, pero requiere mantenimiento si se agregan más meses. Se pueden automatizar con scripts o triggers.

#### En caso de fallo de conexión a PostgreSQL, el script run_analysis.py lanzará una excepción inmediata. No hay reintentos implementados.

#### Es posible adaptar el script a SQLite, aunque la mayoría de las funciones (como CREATE TRIGGER, PARTITION BY) son específicas de PostgreSQL.



### Decisiones técnicas

#### Las consultas están separadas por propósito para facilitar mantenimiento.

#### Se usó psycopg2 para conectar a PostgreSQL desde Python de forma explícita.

#### Los índices fueron diseñados sobre campos de consulta frecuente.

#### La partición por mes mejora rendimiento de análisis históricos o agregaciones.

#### El trigger garantiza consistencia de datos desde la base.




## Ejercicio 3: ETL Python para archivo grande (sample.log.gz)

Este módulo procesa un archivo de logs en formato JSONL comprimido (.gz) con ~5 millones de líneas, utilizando un enfoque streaming eficiente en memoria. El flujo realiza extracción línea por línea, transformación de registros con errores status_code ≥ 500, y exportación en formato Parquet comprimido (snappy).



### Objetivos cumplidos

#### Lectura en streaming desde archivo .gz (sin cargar todo en memoria)
#### Filtrado y validación de registros JSON malformados
#### Transformación a métricas por hora y endpoint
#### Exportación optimizada a formato columnar Parquet
#### Medición de tiempos y memoria (profiling)
#### Manejo de errores y logging estandarizado
#### Preparado para procesamiento por lotes o paralelismo en versiones futuras



### Descripción de las tareas

#### Lectura eficiente del archivo .gz

##### Se implementó read_log_file() en extract_log.py para procesar el archivo sample.log.gz línea por línea, parseando solo registros JSON válidos sin cargar todo en memoria.

#### Filtrado y transformación de errores

##### En transform_log.py, se filtran registros con status_code >= 500, se redondea el timestamp a la hora y se agrupan por hour y endpoint para calcular el total de errores y el tiempo promedio de respuesta.

#### Exportación optimizada a Parquet

##### El resultado se exporta como Parquet comprimido (snappy) a outputs/errors_summary.parquet usando PyArrow para eficiencia en almacenamiento y lectura.

#### Medición de rendimiento

##### Se incluyeron decoradores en profiler.py para medir tiempo de ejecución y pico de memoria con tracemalloc, visibles al finalizar el pipeline.

#### Logging y manejo de errores

##### El pipeline tiene manejo de errores robusto y logs detallados en cada etapa. Registros inválidos no interrumpen el flujo y se descartan de forma controlada.



### Estructura del módulo

| Archivo                    | Función                                                       |
| -------------------------- | ------------------------------------------------------------- |
| `etl_log/extract_log.py`   | Lectura del `.gz` en streaming y validación de registros JSON |
| `etl_log/transform_log.py` | Limpieza, filtrado y agregación por hora y endpoint           |
| `etl_log/export_log.py`    | Exportación a Parquet con compresión Snappy                   |
| `etl_log/profiler.py`      | Medición de tiempo y memoria para funciones críticas          |
| `run_log_etl.py`           | Orquestador del pipeline de logs en ejecución local           |



### Flujo del pipeline

#### Extracción (streaming gzip)

##### Se lee sample.log.gz línea a línea sin cargar todo el archivo.

##### Se parsean solo los objetos JSON válidos.

##### Se descartan registros incompletos o mal formateados.

#### Transformación

##### Se filtran registros con status_code >= 500.

##### Se redondea timestamp a la hora.

##### Se agrupan los datos por hour y endpoint, calculando:

##### Total de requests (count)

##### Tiempo promedio de respuesta (avg_response_time)

#### Exportación

##### Se exporta el DataFrame resultante como archivo Parquet con compresión snappy a la carpeta outputs/.

#### Métricas de ejecución

##### Se mide tiempo y memoria pico usada durante la transformación.



### Ejecución del pipeline

python run_log_etl.py



### Validaciones y observaciones

#### El pipeline funciona incluso con archivos grandes gracias a la lectura en streaming.

#### Los registros malformados no detienen el flujo, son loggeados y descartados.

#### Si no se encuentran registros válidos, no se genera archivo de salida.

#### A futuro se puede adaptar para procesamiento paralelo con multiprocessing, polars o dask.

### Decisiones técnicas

#### Se usó gzip y json para lectura eficiente en línea.

#### Se evita cargar todo el archivo en memoria.

#### Se usa pandas para transformación simple y flexible.

#### Se emplea pyarrow + snappy para salida comprimida y rápida.

#### El diseño modular permite escalar o paralelizar el proceso en siguientes ejercicios.




## Ejercicio 4: Modelado de datos

Este módulo implementa un modelo estrella (star schema) basado en el archivo sample_transactions.csv, siguiendo principios de modelado dimensional y buenas prácticas para análisis OLAP. El proceso incluye el diseño de tablas de dimensiones y hecho, la carga inicial de datos desde CSV, estrategias de control de cambios (SCD), y simulación de partición lógica para optimizar consultas y mantenimiento.



✅ Objetivos cumplidos

✔️ Modelado estrella con tablas de hechos y dimensiones
✔️ Carga de datos desde CSV a modelo dimensional
✔️ Estrategia SCD tipo 1 implementada en dimensión usuarios
✔️ Simulación de partición lógica y archivo
✔️ Comentarios y documentación en código
✔️ Script principal automatizado y modular
✔️ Separación clara por componentes: SQL, scripts de carga, estrategia SCD



### Descripción de las tareas

#### Diseño de esquema estrella

##### Se diseñó un esquema de datos basado en las siguientes entidades:

##### Tabla de hechos fact_transactions. Contiene las transacciones realizadas por los usuarios, incluyendo fecha, monto, estado, y claves foráneas a dimensiones.

#### Dimensión dim_users

##### Contiene información única por usuario (user_id, name, email). Se implementa estrategia SCD tipo 1.

#### Dimensión dim_time

##### Extrae y organiza componentes temporales de las fechas de transacción (día, mes, año, trimestre, etc.).

#### Carga desde CSV

##### Se utilizan scripts Python con Pandas y SQLAlchemy para cargar datos desde el archivo sample_transactions.csv a las tablas del modelo.

##### Cada dimensión se carga primero, seguida de la tabla de hechos, enlazando con las dimensiones por claves foráneas.

#### Estrategia SCD (Slowly Changing Dimensions)

##### SCD Tipo 1: Se implementa en dim_users, sobrescribiendo valores antiguos en caso de cambios (actualización directa).

##### Se valida existencia previa del usuario antes de insertar o actualizar.

#### Simulación de partición lógica

##### La tabla fact_transactions se puede dividir lógicamente por transaction_date (mes o trimestre).

##### Se propone usar vistas o subconsultas por rangos de fecha en escenarios con grandes volúmenes de datos.

#####Alternativamente, se puede emplear un campo partition_key para facilitar segmentación futura.



### Estructura del módulo

| Archivo                                      | Función                                                          |
| -------------------------------------------- | ---------------------------------------------------------------- |
| `modeling/schema_star.sql`                   | Script para crear las tablas de hecho y dimensiones              |
| `modeling/scripts/load_dim_users.py`         | Carga de usuarios desde CSV con SCD tipo 1                       |
| `modeling/scripts/load_dim_time.py`          | Generación y carga de la dimensión temporal                      |
| `modeling/scripts/load_fact_transactions.py` | Inserción de datos en tabla de hechos                            |
| `modeling/scripts/run_star_model.py`                 | Script principal que orquesta todo el proceso de carga al modelo |



### Flujo del pipeline

#### Creación del esquema

##### Se ejecuta schema_star.sql para crear las tablas necesarias.

#### Carga de dimensiones

##### dim_users: Cargada desde CSV con validación y estrategia SCD.

##### dim_time: Generada a partir de fechas únicas encontradas en el CSV.

#### Carga de hechos

##### Se extraen IDs desde las dimensiones para poblar fact_transactions correctamente.

#### Simulación de partición

##### Puede realizarse mediante subconsultas por transaction_date o usando un campo partition_key.



### Ejecución del pipeline

python -m modeling.scripts.run_star_model

Este script se encarga de ejecutar los tres scripts de carga (load_dim_users, load_dim_time, load_fact_transactions) en orden lógico, mostrando mensajes de estado en consola.



###  Validaciones y observaciones

#### Si las tablas ya existen, el script schema_star.sql las reemplaza usando DROP TABLE IF EXISTS.

#### La dimensión tiempo se construye con base en las fechas reales del archivo CSV.

#### La tabla de hechos solo se llena si todas las claves foráneas están presentes.

#### La estrategia SCD tipo 1 permite mantener los datos actualizados sobrescribiendo versiones anteriores sin duplicados.

#### En casos reales, la estrategia SCD tipo 2 podría ser preferible si se desea conservar historial.



### Decisiones técnicas

#### Se optó por SCD tipo 1 para simplificar la implementación sin perder la integridad de datos.

#### El modelo estrella fue elegido por su simplicidad, rendimiento en consultas analíticas y buena adaptabilidad a OLAP.

#### Se evitó el uso de ORMs complejos para mantener control total sobre las inseZrciones SQL.

#### El script python -m modeling.scripts.run_star_model permite automatizar todo el flujo de forma clara y replicable.





## Ejercicio 5: Git + CI/CD

Este módulo integra todos los componentes del proyecto en una estructura de repositorio organizada, con flujo de trabajo reproducible y automatizado. Se aplican prácticas de control de versiones profesional, integración continua, validaciones de calidad de código y preparación para despliegues reproducibles en contenedores o entornos remotos.



###  Objetivos cumplidos

✔️ Estructura modular del repositorio
✔️ Flujo de Git profesional con ramas temáticas
✔️ Integración continua con GitHub Actions
✔️ Validaciones automáticas: linting, test y formateo
✔️ Pipeline preparado para ejecutar ETL y SQL
✔️ Comentarios inline y scripts listos para extensión
✔️ Preparado para contenerización y despliegue (extensible)



### Descripción de las tareas

#### Organización del repositorio
 
##### Se definió una estructura clara por módulos (etl/, etl_log/, sql/, modeling/, ci/), junto con un README.md detallado, archivo .env.example y carpeta tests/ como placeholder para futuras pruebas.

#### Flujo de trabajo Git

##### Se estableció un flujo de ramas profesional con ramas main, dev y feature/<nombre> para organizar el desarrollo y facilitar merges limpios y revisados.

#### Configuración de CI/CD

##### Se implementó un pipeline básico con GitHub Actions (.github/workflows/ci.yml) que incluye las siguientes etapas:

#### Instalación de dependencias

#### Validación de estilo con flake8

#### Validación de formato con black

#### Validaciones automáticas

##### El pipeline valida automáticamente el código fuente para asegurar calidad antes de hacer merge a main. El estilo y formato se aplican de forma homogénea en todos los módulos.

#### Preparación para contenerización

##### Aunque no se entrega una imagen Docker ni archivos Dockerfile o Makefile, la estructura del proyecto permite añadirlos fácilmente si se decide escalar el entorno a contenedores o despliegue remoto.



Estructura del repositorio

| Carpeta / Archivo    | Función principal                                                 |
| -------------------- | ----------------------------------------------------------------- |
| `etl/`               | Módulos de extracción, transformación y carga (Ejercicio 1)       |
| `etl_log/`           | ETL en streaming para logs grandes (Ejercicio 3)                  |
| `sql/`               | Consultas, vistas, triggers e índices SQL (Ejercicio 2)           |
| `modeling/`          | Modelo en estrella y scripts de carga (Ejercicio 4)               |
| `ci/`                | Archivos de configuración para CI/CD                              |
| `.github/workflows/` | Pipeline de CI con GitHub Actions                                 |
| `requirements.txt`   | Dependencias del proyecto                                         |
| `.env.example`       | Plantilla de variables de entorno                                 |
| `README.md`          | Documentación principal del repositorio                           |
| `tests/`             | Carpeta preparada para pruebas unitarias y de integración futuras |



### Ejecución del pipeline local

#### Simular pipeline (modo manual)

python main.py
python run_analysis.py
python run_log_etl.py
python -m modeling.scripts.run_star_model



### Validaciones y observaciones

#### Las rutas de scripts y carpetas están organizadas para ejecución reproducible.

#### Se incluye .env.example como guía segura sin exponer credenciales.

#### El pipeline CI permite detectar errores antes del merge a main.

#### Las pruebas aún no se incluyen pero el proyecto está listo para agregarlas.

#### El flujo es extensible a Docker o Prefect si se desea ampliar.



### Decisiones técnicas

#### Se optó por GitHub Actions por su simplicidad y soporte directo en proyectos públicos.

#### Se evitó contenerización en esta etapa para mantener foco en reproducibilidad local.

#### La modularidad del código facilita integración de pruebas unitarias y de integración.

#### El pipeline está diseñado para crecer con la madurez del proyecto (testing, Docker, deploy).