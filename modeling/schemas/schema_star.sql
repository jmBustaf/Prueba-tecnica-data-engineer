-- dim_users: tabla de dimensión para los usuarios
-- Contiene un identificador único por usuario.
CREATE TABLE IF NOT EXISTS dim_users (
    user_id INT PRIMARY KEY
);

-- dim_date: tabla de dimensión de tiempo
-- Incluye campos desnormalizados para facilitar agregaciones por día, mes, año o día de la semana.
CREATE TABLE IF NOT EXISTS dim_date (
    date_id SERIAL PRIMARY KEY,      -- Identificador único (surrogate key)
    full_date DATE UNIQUE,           -- Fecha completa (YYYY-MM-DD)
    day INT,                         -- Día del mes
    month INT,                       -- Mes numérico
    year INT,                        -- Año numérico
    weekday TEXT                     -- Nombre del día de la semana (Ej: 'Monday')
);

-- fact_transactions: tabla de hechos
-- Almacena eventos de transacciones vinculados a las dimensiones de usuario y fecha.
CREATE TABLE IF NOT EXISTS fact_transactions (
    transaction_id INT PRIMARY KEY,                  -- ID único por transacción
    user_id INT REFERENCES dim_users(user_id),       -- FK a la dimensión de usuario
    date_id INT REFERENCES dim_date(date_id),        -- FK a la dimensión de fecha
    amount NUMERIC,                                  -- Monto de la transacción
    status TEXT,                                     -- Estado (Ej: 'success', 'failed')
    ts TIMESTAMP                                     -- Timestamp completo del evento
);

