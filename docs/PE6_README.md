# PE6 — Redshift Streaming Ingestion + Consulta Iceberg

## Arquitectura

```
                    Kinesis Data Streams
                           │
            ┌──────────────┴──────────────┐
            │                             │
            ▼                             ▼
  Redshift Serverless               Managed Flink (PE4)
  ┌─────────────────────┐                │
  │ External Schema     │                ▼
  │ FROM KINESIS        │          Iceberg → S3 (PE5)
  │                     │                │
  │ mv_kinesis_         │                ▼
  │ transactions        │          Glue Data Catalog
  │ (AUTO REFRESH YES)  │                │
  │                     │                ▼
  │ 6 campos JSON:      │     ┌──────────────────┐
  │  event_id           │     │ External Schema   │
  │  event_time         │     │ FROM DATA CATALOG │
  │  merchant_id        │     │                   │
  │  amount             │     │ lakehouse.        │
  │  currency           │     │   transactions    │
  │  status             │     │   merchant_       │
  │                     │     │   aggregations    │
  │      JOIN ──────────┼─────┤                   │
  │                     │     └──────────────────┘
  └─────────────────────┘
```

## Esquemas externos (distintos)

| Schema | Tipo | Propósito |
|---|---|---|
| `kinesis_streaming` | `FROM KINESIS` | Streaming Ingestion directa |
| `lakehouse` | `FROM DATA CATALOG` | Consulta tablas Iceberg (Spectrum) |

**Nota crítica**: son mecanismos completamente distintos.
`FROM KINESIS` lee el stream en tiempo real; `FROM DATA CATALOG` consulta
datos históricos en S3 vía Glue. No se debe usar `FROM DATA CATALOG`
para streaming ingestion.

## Materialized View

```sql
CREATE MATERIALIZED VIEW mv_kinesis_transactions
AUTO REFRESH YES
AS
SELECT
    approximate_arrival_timestamp,
    partition_key,
    shard_id,
    sequence_number,
    -- 6 campos JSON (rúbrica exige ≥3)
    JSON_EXTRACT_PATH_TEXT(from_varbyte(kinesis_data, 'utf-8'), 'event_id')    AS event_id,
    CAST(JSON_EXTRACT_PATH_TEXT(..., 'event_time') AS TIMESTAMP)               AS event_time,
    JSON_EXTRACT_PATH_TEXT(..., 'merchant_id')                                 AS merchant_id,
    CAST(JSON_EXTRACT_PATH_TEXT(..., 'amount') AS DECIMAL(18,2))               AS amount,
    JSON_EXTRACT_PATH_TEXT(..., 'currency')                                    AS currency,
    JSON_EXTRACT_PATH_TEXT(..., 'status')                                      AS status
FROM kinesis_streaming."streaming-data-platform-events-dev";
```

- **AUTO REFRESH YES**: Redshift actualiza automáticamente la MV
- **6 campos**: event_id, event_time, merchant_id, amount, currency, status
- Metadata Kinesis incluida: arrival_timestamp, partition_key, shard_id, sequence_number

## Estructura de archivos

```
terraform/modules/redshift/
  main.tf               # Namespace + Workgroup + SG + IAM (Kinesis + Glue + S3 + KMS)
  variables.tf          # Variables del módulo
  outputs.tf            # Outputs del módulo
sql/
  01_streaming_schema.sql     # External schema FROM KINESIS
  02_materialized_view.sql    # MV con 6 campos JSON + AUTO REFRESH
  03_external_schema_glue.sql # External schema FROM DATA CATALOG
  04_queries_and_joins.sql    # Consultas + JOIN streaming × Iceberg
  05_refresh_and_monitoring.sql # REFRESH, métricas, latencia
docs/
  PE6_README.md               # Este archivo
```

## SQL — Orden de ejecución

1. `01_streaming_schema.sql` — Crear schema `kinesis_streaming` FROM KINESIS
2. `02_materialized_view.sql` — Crear MV base con 6 campos + AUTO REFRESH
3. `03_external_schema_glue.sql` — Crear schema `lakehouse` FROM DATA CATALOG
4. `04_queries_and_joins.sql` — Consultas analíticas + JOINs
5. `05_refresh_and_monitoring.sql` — REFRESH manual + métricas

## JOIN: Streaming × Iceberg

El JOIN principal cruza eventos en tiempo real (MV Kinesis) con
agregaciones históricas (Iceberg vía Spectrum):

```sql
SELECT
    mv.event_id, mv.merchant_id, mv.amount,
    agg.avg_amount AS merchant_window_avg,
    CASE
        WHEN mv.amount > agg.avg_amount * 2 THEN 'HIGH'
        WHEN mv.amount < agg.avg_amount * 0.5 THEN 'LOW'
        ELSE 'NORMAL'
    END AS amount_category
FROM mv_kinesis_transactions mv
LEFT JOIN lakehouse.merchant_aggregations agg
    ON mv.merchant_id = agg.merchant_id
   AND mv.event_time >= agg.window_start
   AND mv.event_time < agg.window_end;
```

Esto permite detectar anomalías comparando cada transacción con
el promedio histórico de su merchant en la ventana correspondiente.

## Refresh y latencia

- **AUTO REFRESH YES**: Redshift gestiona la frecuencia de refresh
- **REFRESH manual**: `REFRESH MATERIALIZED VIEW mv_kinesis_transactions`
- **Freshness**: `DATEDIFF(second, MAX(arrival), GETDATE())`
- **Latencia E2E**: `DATEDIFF(ms, event_time, arrival_timestamp)`
- Consultas de monitoreo: `SYS_MV_STATE`, `SYS_MV_REFRESH_HISTORY`

**Nota**: AUTO REFRESH no es instantáneo. Existe un lag entre la
llegada del dato al stream y su visibilidad en la MV. Redshift
**no transmite backpressure directamente a Flink ni a Kinesis**
— son consumidores independientes que comparten los mismos shards.

## Terraform — Recursos declarados

| Recurso | Tipo | Propósito |
|---|---|---|
| Namespace | `aws_redshiftserverless_namespace` | Namespace serverless |
| Workgroup | `aws_redshiftserverless_workgroup` | Workgroup (8 RPU base) |
| Security Group | `aws_security_group` | Acceso puerto 5439 |
| IAM Kinesis | `aws_iam_role_policy` | Streaming Ingestion |
| IAM Glue | `aws_iam_role_policy` | Spectrum / Iceberg |
| IAM S3 | `aws_iam_role_policy` | Lectura lakehouse |
| IAM KMS | `aws_iam_role_policy` | Descifrado S3 |

## IAM — Mínimo privilegio

- **Kinesis**: solo lectura (`GetRecords`, `GetShardIterator`, `DescribeStream`) + consumer management
- **Glue**: solo lectura del catálogo (`GetDatabase`, `GetTable`, `GetPartition`)
- **S3**: solo lectura del lakehouse (`GetObject`, `ListBucket`)
- **KMS**: solo `Decrypt` y `GenerateDataKey` para la llave del proyecto
- Sin permisos de escritura en ningún recurso externo

## Limitaciones académicas

| Aspecto | Estado |
|---|---|
| Terraform HCL válido | ✅ `terraform validate` en CI |
| SQL revisado estáticamente | ✅ Sintaxis Redshift verificada |
| Esquema Kinesis y Glue distintos | ✅ Documentado y separado |
| MV con ≥3 campos | ✅ 6 campos JSON |
| AUTO REFRESH configurado | ✅ En SQL |
| JOIN streaming × Iceberg | ✅ En SQL |
| Creación Redshift Serverless | `BLOQUEADO_POR_COSTO` |
| Ejecución SQL real | `BLOQUEADO_POR_COSTO` |
| Métricas de latencia reales | `BLOQUEADO_POR_COSTO` |
| Refresh verificado | `BLOQUEADO_POR_COSTO` |

## Criterios de rúbrica PE6

| Criterio | Peso | Estado |
|---|---|---|
| Streaming schema FROM KINESIS + MV ≥3 campos | 35% | `IMPLEMENTED_UNVERIFIED` |
| External schema FROM DATA CATALOG + JOIN | 25% | `IMPLEMENTED_UNVERIFIED` |
| REFRESH / AUTO REFRESH + métricas latencia | 15% | `IMPLEMENTED_UNVERIFIED` |
| IAM mínimo privilegio | 15% | `IMPLEMENTED_UNVERIFIED` |
| SQL, documentación y evidencia | 10% | `IMPLEMENTED_UNVERIFIED` |
