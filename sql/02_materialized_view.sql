-- ============================================================
-- PE6 — Paso 2: Materialized View base sobre Kinesis Stream
--
-- Lee datos JSON directamente del stream Kinesis y los
-- materializa como tabla consultable en Redshift.
--
-- Campos extraídos del JSON (≥3 requeridos por rúbrica):
--   1. event_id
--   2. event_time
--   3. merchant_id
--   4. amount
--   5. currency
--   6. status
--
-- AUTO REFRESH: Redshift actualiza la MV automáticamente
-- cuando hay nuevos registros en el stream.
--
-- BLOQUEADO_POR_COSTO: requiere Redshift + Kinesis activos.
-- ============================================================

-- MV base: parseo JSON de eventos desde Kinesis
CREATE MATERIALIZED VIEW mv_kinesis_transactions
AUTO REFRESH YES
AS
SELECT
    -- Metadata de Kinesis
    approximate_arrival_timestamp,
    partition_key,
    shard_id,
    sequence_number,

    -- Campos del payload JSON (≥3 requeridos)
    JSON_EXTRACT_PATH_TEXT(
        from_varbyte(kinesis_data, 'utf-8'), 'event_id'
    ) AS event_id,

    CAST(JSON_EXTRACT_PATH_TEXT(
        from_varbyte(kinesis_data, 'utf-8'), 'event_time'
    ) AS TIMESTAMP) AS event_time,

    JSON_EXTRACT_PATH_TEXT(
        from_varbyte(kinesis_data, 'utf-8'), 'merchant_id'
    ) AS merchant_id,

    CAST(JSON_EXTRACT_PATH_TEXT(
        from_varbyte(kinesis_data, 'utf-8'), 'amount'
    ) AS DECIMAL(18,2)) AS amount,

    JSON_EXTRACT_PATH_TEXT(
        from_varbyte(kinesis_data, 'utf-8'), 'currency'
    ) AS currency,

    JSON_EXTRACT_PATH_TEXT(
        from_varbyte(kinesis_data, 'utf-8'), 'status'
    ) AS status

FROM kinesis_streaming."streaming-data-platform-events-dev";

-- Verificar MV creada
SELECT
    mv_name,
    schema_name,
    db_name,
    is_stale,
    state,
    autorefresh
FROM SYS_MV_STATE
WHERE mv_name = 'mv_kinesis_transactions';
