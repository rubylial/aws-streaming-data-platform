-- ============================================================
-- PE6 — Paso 2: Materialized View base sobre Kinesis Stream
--
-- Lee datos JSON directamente del stream Kinesis y los
-- materializa como tabla consultable en Redshift.
--
-- Campos extraídos del JSON (≥3 requeridos por rúbrica):
--   1. event_id   4. amount
--   2. event_time 5. currency
--   3. merchant_id 6. status
--
-- AUTO REFRESH YES: Redshift actualiza la MV automáticamente.
--
-- IMPORTANTE (validado en AWS 2026-11-12):
--   event_time se deja como VARCHAR (texto). El CAST a TIMESTAMP
--   es una función MUTABLE y Redshift NO permite AUTO REFRESH sobre
--   MVs con funciones mutables:
--     "Auto-refresh is not supported for materialized views defined
--      on mutable functions. Found mutable function: cast from VARCHAR
--      to TIMESTAMP."
--   Por eso el cast a timestamp se hace en las consultas downstream
--   (ver 04_queries_and_joins.sql y 05_refresh_and_monitoring.sql).
--
-- BLOQUEADO_POR_COSTO: requiere Redshift + Kinesis activos.
-- Validado en AWS Academy (Redshift ra3.large) el 2026-11-12.
-- ============================================================

CREATE MATERIALIZED VIEW mv_kinesis_transactions
AUTO REFRESH YES
AS
SELECT
    -- Metadata de Kinesis (procedencia / trazabilidad)
    approximate_arrival_timestamp,
    partition_key,
    shard_id,
    sequence_number,

    -- Campos del payload JSON (≥3 requeridos)
    JSON_EXTRACT_PATH_TEXT(
        from_varbyte(kinesis_data, 'utf-8'), 'event_id'
    ) AS event_id,

    -- event_time como TEXTO (evita función mutable; cast en downstream)
    JSON_EXTRACT_PATH_TEXT(
        from_varbyte(kinesis_data, 'utf-8'), 'event_time'
    ) AS event_time,

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

-- Verificar MV creada y estado de auto-refresh
SELECT * FROM STV_MV_INFO
WHERE name = 'mv_kinesis_transactions';
