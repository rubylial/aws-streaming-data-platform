-- ============================================================
-- PE6 — Paso 1: External Schema FROM KINESIS
--
-- Crea el esquema externo apuntando directamente al
-- Kinesis Data Stream para Streaming Ingestion.
--
-- IMPORTANTE: Este esquema usa FROM KINESIS, NO FROM DATA CATALOG.
-- Son mecanismos distintos:
--   - FROM KINESIS: lectura directa del stream (streaming ingestion)
--   - FROM DATA CATALOG: acceso a tablas Iceberg/Glue (Spectrum)
--
-- BLOQUEADO_POR_COSTO: requiere Redshift Serverless activo.
-- ============================================================

-- Crear external schema para Streaming Ingestion desde Kinesis
CREATE EXTERNAL SCHEMA IF NOT EXISTS kinesis_streaming
FROM KINESIS
IAM_ROLE 'arn:aws:iam::ACCOUNT_ID:role/streaming-data-platform-redshift-role-dev';

-- Verificar schema creado
SELECT
    schemaname,
    databasename,
    esoptions
FROM svv_external_schemas
WHERE schemaname = 'kinesis_streaming';
