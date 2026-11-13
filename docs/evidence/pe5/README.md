# Evidencias PE5 — Validación AWS Academy Lab

**Fecha:** 2026-11-12  
**Entorno:** AWS Academy Learner Lab (us-east-1)  
**Costo:** ~$0.05 USD (créditos académicos)

## Capturas

| # | Archivo | Descripción |
|---|---|---|
| 1 | `01-glue-database.png` | Glue database streaming_data_platform_dev creada |
| 2 | `02-tabla-transactions.png` | Tabla transactions formato Iceberg con columnas |
| 3 | `03-tabla-merchant-aggregations.png` | Tabla merchant_aggregations formato Iceberg |
| 4 | `04-athena-show-tables.png` | Athena: SHOW TABLES |
| 5 | `05-athena-describe-transactions.png` | Athena: DESCRIBE transactions |
| 6 | `06-athena-describe-aggregations.png` | Athena: DESCRIBE merchant_aggregations |
| 7 | `07-athena-count.png` | Athena: SELECT COUNT(*) = 0 |
| 8 | `08-athena-recent-queries.png` | Athena: historial de queries |
| 9 | `09-limpieza.png` | Confirmación de limpieza completa |

## Nota

Las tablas están vacías (COUNT = 0) porque Flink no fue ejecutado en AWS.
Esto demuestra que Glue Catalog + Athena reconocen y consultan tablas Iceberg correctamente.
