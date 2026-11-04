-- ============================================================
-- PE5 — Consultas Athena sobre tablas Iceberg en Glue Catalog
--
-- Base de datos: streaming_data_platform_dev (Glue)
-- Tablas: transactions, merchant_aggregations
-- Formato: Iceberg v2 sobre S3 (Parquet + Snappy)
--
-- BLOQUEADO_POR_COSTO: estas consultas requieren Athena + Glue + S3.
-- Se presentan como validación preparada, NO ejecutada.
-- ============================================================

-- ---------------------------------------------------------
-- 1. Verificar metadata de la tabla Iceberg
-- ---------------------------------------------------------

-- Listar tablas en el catálogo
SHOW TABLES IN streaming_data_platform_dev;

-- Describir esquema de transactions
DESCRIBE streaming_data_platform_dev.transactions;

-- Describir esquema de merchant_aggregations
DESCRIBE streaming_data_platform_dev.merchant_aggregations;

-- ---------------------------------------------------------
-- 2. Consultas sobre transactions (eventos individuales)
-- ---------------------------------------------------------

-- Contar total de eventos procesados
SELECT COUNT(*) AS total_events
FROM streaming_data_platform_dev.transactions;

-- Distribución por merchant
SELECT
    merchant_id,
    COUNT(*)       AS event_count,
    SUM(amount)    AS total_amount,
    AVG(amount)    AS avg_amount,
    MIN(event_time) AS first_event,
    MAX(event_time) AS last_event
FROM streaming_data_platform_dev.transactions
GROUP BY merchant_id
ORDER BY total_amount DESC;

-- Eventos por status
SELECT
    status,
    COUNT(*) AS count,
    ROUND(AVG(amount), 2) AS avg_amount
FROM streaming_data_platform_dev.transactions
GROUP BY status;

-- Verificar duplicados por event_id (idempotencia)
SELECT
    event_id,
    COUNT(*) AS occurrences
FROM streaming_data_platform_dev.transactions
GROUP BY event_id
HAVING COUNT(*) > 1;

-- ---------------------------------------------------------
-- 3. Consultas sobre merchant_aggregations (ventanas)
-- ---------------------------------------------------------

-- Últimas agregaciones por merchant
SELECT
    merchant_id,
    window_start,
    window_end,
    transaction_count,
    total_amount,
    avg_amount
FROM streaming_data_platform_dev.merchant_aggregations
ORDER BY window_end DESC
LIMIT 20;

-- Tendencia por merchant (últimas 10 ventanas)
SELECT
    merchant_id,
    window_start,
    transaction_count,
    total_amount,
    avg_amount,
    total_amount - LAG(total_amount) OVER (
        PARTITION BY merchant_id ORDER BY window_start
    ) AS amount_delta
FROM streaming_data_platform_dev.merchant_aggregations
ORDER BY merchant_id, window_start DESC;

-- ---------------------------------------------------------
-- 4. Metadata Iceberg (snapshots, history)
-- ---------------------------------------------------------

-- Historial de snapshots (commits Iceberg)
SELECT *
FROM streaming_data_platform_dev."transactions$snapshots"
ORDER BY committed_at DESC;

-- Historial de manifests
SELECT *
FROM streaming_data_platform_dev."transactions$manifests";

-- Archivos de datos (verificar compactación)
SELECT
    file_path,
    file_format,
    record_count,
    file_size_in_bytes
FROM streaming_data_platform_dev."transactions$files"
ORDER BY file_size_in_bytes DESC
LIMIT 10;

-- ---------------------------------------------------------
-- 5. Time Travel (consulta histórica Iceberg)
-- ---------------------------------------------------------

-- Consultar estado de la tabla en un snapshot específico
-- (reemplazar <snapshot_id> por ID real del snapshot)
-- SELECT COUNT(*) FROM streaming_data_platform_dev.transactions
-- FOR VERSION AS OF <snapshot_id>;

-- Consultar estado a una fecha específica
-- SELECT COUNT(*) FROM streaming_data_platform_dev.transactions
-- FOR TIMESTAMP AS OF TIMESTAMP '2024-11-04 05:00:00';
