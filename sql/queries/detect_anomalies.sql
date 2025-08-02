-- Detección de anomalías: días donde las transacciones exceden 2 desviaciones estándar
WITH daily_counts AS (
  SELECT DATE(ts::timestamp) AS day, COUNT(*) AS total
  FROM transactions
  GROUP BY day
),
avg_and_stddev AS (
  SELECT
    AVG(total) AS avg_total,
    STDDEV(total) AS std_dev
  FROM daily_counts
),
latest_day AS (
  SELECT DATE(MAX(ts::timestamp)) AS latest
  FROM transactions
)
SELECT d.day, d.total, a.avg_total, a.std_dev
FROM daily_counts d
CROSS JOIN avg_and_stddev a
JOIN latest_day l ON d.day = l.latest
WHERE d.total > a.avg_total + 2 * a.std_dev;
