# Evidencias PE6 — Redshift Streaming Ingestion

**Fecha:** 2026-11-12 (UTC 2026-11-13)
**Entorno:** AWS Academy Learner Lab — us-east-1
**Recurso:** Redshift Provisioned ra3.large (1 nodo)
**Cuenta:** [ANONIMIZADA]
**Costo:** ~$3-4 USD (créditos académicos)

## Capturas (ver Evidencia_PE6.pdf)

| # | Query / Captura | Criterio | Resultado |
|---|---|---|---|
| 1 | CREATE EXTERNAL SCHEMA FROM KINESIS | PE6-01 (35%) | OK |
| 2 | CREATE MV mv_kinesis_transactions (6 campos, AUTO REFRESH YES) | PE6-01 | OK |
| 3 | REFRESH + SELECT COUNT(*) | PE6-01 | 50 eventos |
| 4 | Distribución por merchant | PE6-01 | 5 merchants |
| 5 | STV_MV_INFO | PE6-03 | autorefresh=t |
| 6 | CREATE EXTERNAL SCHEMA FROM DATA CATALOG | PE6-02 (25%) | OK |
| 7 | svv_external_tables | PE6-02 | 2 tablas Iceberg |
| 8 | SELECT COUNT Iceberg | PE6-02 | 0 (Flink no corrió) |
| 9 | JOIN streaming × Iceberg | PE6-02 | OK |
| 10 | Métricas de latencia (min/avg/max) | PE6-03 (15%) | OK |
| 11 | Freshness | PE6-03 | OK |
| 12 | SYS_MV_REFRESH_HISTORY | PE6-03 | OK |
| 13 | svv_external_schemas (ambos) | PE6-01/02 | kinesis_streaming + lakehouse |
| 14 | MV con procedencia (event_id + sequence_number + shard_id) | DAAT-02 | Trazabilidad parcial |
| 15 | Detalle clúster (ra3.large, Available) | PE6 | Despliegue real |

## Notas

- Redshift Serverless bloqueado en Academy → se usó Provisioned ra3.large.
- AUTO REFRESH requirió dejar event_time como texto (el CAST a TIMESTAMP es
  función mutable y bloquea auto-refresh). El cast se hace en consultas downstream.
- Tablas Iceberg vacías (Flink no ejecutado en AWS); el JOIN demuestra que
  ambos external schemas funcionan.
- Limpieza completa de recursos confirmada.
