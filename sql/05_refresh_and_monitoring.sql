-- ============================================================
-- PE6 — Paso 5: REFRESH, métricas y monitoreo de latencia
--
-- Demuestra:
-- 1. REFRESH manual de la MV
-- 2. Verificación del estado AUTO REFRESH
-- 3. Métricas de freshness y latencia
-- 4. Monitoreo de la MV
--
-- BLOQUEADO_POR_COSTO: requiere Redshift + Kinesis activos.
-- ============================================================

-- ---------------------------------------------------------
-- 1. REFRESH manual
-- ---------------------------------------------------------

-- Forzar refresh manual de la MV (además del AUTO REFRESH)
REFRESH MATERIALIZED VIEW mv_kinesis_transactions;

-- ---------------------------------------------------------
-- 2. Estado del AUTO REFRESH
-- ---------------------------------------------------------

-- Verificar configuración de auto refresh
SELECT
    mv_name,
    schema_name,
    is_stale,
    state,
    autorefresh
FROM SYS_MV_STATE
WHERE mv_name = 'mv_kinesis_transactions';

-- Historial de refreshes
SELECT
    mv_name,
    starttime,
    endtime,
    DATEDIFF(second, starttime, endtime) AS refresh_duration_sec,
    status,
    rows_inserted
FROM SYS_MV_REFRESH_HISTORY
WHERE mv_name = 'mv_kinesis_transactions'
ORDER BY starttime DESC
LIMIT 20;

-- ---------------------------------------------------------
-- 3. Latencia / Freshness
-- ---------------------------------------------------------

-- Medir latencia: diferencia entre event_time y arrival_timestamp
SELECT
    MIN(DATEDIFF(millisecond, CAST(event_time AS TIMESTAMP), approximate_arrival_timestamp)) AS min_latency_ms,
    AVG(DATEDIFF(millisecond, CAST(event_time AS TIMESTAMP), approximate_arrival_timestamp)) AS avg_latency_ms,
    MAX(DATEDIFF(millisecond, CAST(event_time AS TIMESTAMP), approximate_arrival_timestamp)) AS max_latency_ms,
    PERCENTILE_CONT(0.95) WITHIN GROUP (
        ORDER BY DATEDIFF(millisecond, CAST(event_time AS TIMESTAMP), approximate_arrival_timestamp)
    ) AS p95_latency_ms
FROM mv_kinesis_transactions
WHERE CAST(event_time AS TIMESTAMP) >= DATEADD(hour, -1, GETDATE());

-- Freshness: cuánto tiempo tiene el dato más reciente
SELECT
    MAX(approximate_arrival_timestamp) AS latest_arrival,
    DATEDIFF(second, MAX(approximate_arrival_timestamp), GETDATE())
        AS freshness_seconds,
    COUNT(*) AS total_records
FROM mv_kinesis_transactions;

-- ---------------------------------------------------------
-- 4. Monitoreo de queries
-- ---------------------------------------------------------

-- Últimas queries ejecutadas contra las MVs
SELECT
    query,
    querytxt,
    starttime,
    endtime,
    DATEDIFF(millisecond, starttime, endtime) AS duration_ms
FROM stl_query
WHERE querytxt LIKE '%mv_kinesis_transactions%'
   OR querytxt LIKE '%lakehouse%'
ORDER BY starttime DESC
LIMIT 10;

-- Uso de recursos de la MV
SELECT
    mv_name,
    SUM(rows_inserted) AS total_rows_refreshed,
    COUNT(*)           AS refresh_count,
    AVG(DATEDIFF(second, starttime, endtime)) AS avg_refresh_sec
FROM SYS_MV_REFRESH_HISTORY
WHERE mv_name = 'mv_kinesis_transactions'
GROUP BY mv_name;
