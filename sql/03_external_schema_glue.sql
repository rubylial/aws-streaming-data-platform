-- ============================================================
-- PE6 — Paso 3: External Schema FROM DATA CATALOG (Iceberg)
--
-- Crea un esquema externo que apunta al Glue Data Catalog
-- para consultar las tablas Iceberg de PE5 desde Redshift.
--
-- IMPORTANTE: Este es un esquema DISTINTO al de Kinesis.
--   - kinesis_streaming: FROM KINESIS (streaming ingestion)
--   - lakehouse:         FROM DATA CATALOG (consulta Iceberg)
--
-- BLOQUEADO_POR_COSTO: requiere Redshift + Glue + S3 activos.
-- ============================================================

-- Crear external schema para consultar Iceberg vía Glue
CREATE EXTERNAL SCHEMA IF NOT EXISTS lakehouse
FROM DATA CATALOG
DATABASE 'streaming_data_platform_dev'
IAM_ROLE 'arn:aws:iam::ACCOUNT_ID:role/streaming-data-platform-redshift-role-dev';

-- Verificar schema creado
SELECT
    schemaname,
    databasename,
    esoptions
FROM svv_external_schemas
WHERE schemaname = 'lakehouse';

-- Listar tablas disponibles en el catálogo Iceberg
SELECT *
FROM svv_external_tables
WHERE schemaname = 'lakehouse';
