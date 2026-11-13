-- ============================================================
-- PE6 — Paso 4: Consultas analíticas y JOINs
--
-- Demuestra:
-- 1. Consultas sobre la MV (datos streaming en tiempo real)
-- 2. Consultas sobre tablas Iceberg (datos históricos)
-- 3. JOIN entre streaming (MV) e Iceberg (lakehouse)
--
-- BLOQUEADO_POR_COSTO: requiere Redshift + Kinesis + Glue + S3.
-- ============================================================

-- ---------------------------------------------------------
-- A. Consultas sobre MV (streaming Kinesis)
-- ---------------------------------------------------------

-- Total de eventos ingeridos en tiempo real
SELECT COUNT(*) AS total_streaming_events
FROM mv_kinesis_transactions;

-- Distribución por merchant (streaming)
SELECT
    merchant_id,
    COUNT(*)                  AS event_count,
    SUM(amount)               AS total_amount,
    ROUND(AVG(amount), 2)     AS avg_amount,
    MIN(event_time)           AS first_event,
    MAX(event_time)           AS last_event
FROM mv_kinesis_transactions
GROUP BY merchant_id
ORDER BY total_amount DESC;

-- Distribución por status (streaming)
SELECT
    status,
    COUNT(*)              AS count,
    ROUND(AVG(amount), 2) AS avg_amount
FROM mv_kinesis_transactions
GROUP BY status;

-- Últimos 20 eventos (streaming)
SELECT
    event_id,
    event_time,
    merchant_id,
    amount,
    status,
    approximate_arrival_timestamp
FROM mv_kinesis_transactions
ORDER BY approximate_arrival_timestamp DESC
LIMIT 20;

-- ---------------------------------------------------------
-- B. Consultas sobre Iceberg (lakehouse histórico)
-- ---------------------------------------------------------

-- Total eventos Iceberg procesados por Flink
SELECT COUNT(*) AS total_iceberg_events
FROM lakehouse.transactions;

-- Agregaciones por merchant desde Iceberg
SELECT
    merchant_id,
    window_start,
    window_end,
    transaction_count,
    total_amount,
    avg_amount
FROM lakehouse.merchant_aggregations
ORDER BY window_end DESC
LIMIT 20;

-- ---------------------------------------------------------
-- C. JOIN: Streaming (MV Kinesis) + Histórico (Iceberg)
-- ---------------------------------------------------------

-- Comparar conteo streaming vs Iceberg por merchant
SELECT
    COALESCE(s.merchant_id, i.merchant_id) AS merchant_id,
    s.streaming_count,
    s.streaming_total,
    i.iceberg_count,
    i.iceberg_total,
    s.streaming_count - COALESCE(i.iceberg_count, 0) AS count_delta,
    ROUND(s.streaming_total - COALESCE(i.iceberg_total, 0), 2) AS amount_delta
FROM (
    SELECT
        merchant_id,
        COUNT(*)    AS streaming_count,
        SUM(amount) AS streaming_total
    FROM mv_kinesis_transactions
    GROUP BY merchant_id
) s
FULL OUTER JOIN (
    SELECT
        merchant_id,
        SUM(transaction_count) AS iceberg_count,
        SUM(total_amount)      AS iceberg_total
    FROM lakehouse.merchant_aggregations
    GROUP BY merchant_id
) i ON s.merchant_id = i.merchant_id
ORDER BY merchant_id;

-- Enriquecimiento: eventos streaming con agregado Iceberg
SELECT
    mv.event_id,
    mv.event_time,
    mv.merchant_id,
    mv.amount,
    mv.status,
    agg.transaction_count   AS merchant_window_count,
    agg.avg_amount          AS merchant_window_avg,
    CASE
        WHEN mv.amount > agg.avg_amount * 2 THEN 'HIGH'
        WHEN mv.amount < agg.avg_amount * 0.5 THEN 'LOW'
        ELSE 'NORMAL'
    END AS amount_category
FROM mv_kinesis_transactions mv
LEFT JOIN lakehouse.merchant_aggregations agg
    ON mv.merchant_id = agg.merchant_id
   AND CAST(mv.event_time AS TIMESTAMP) >= agg.window_start
   AND CAST(mv.event_time AS TIMESTAMP) < agg.window_end
ORDER BY mv.event_time DESC
LIMIT 50;
