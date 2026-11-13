# Evidencias PE2 — Kinesis + Firehose

**Fecha:** 2026-11-12 (UTC 2026-11-13)
**Entorno:** AWS Academy Learner Lab — us-east-1
**Cuenta:** [ANONIMIZADA]

## Capturas (ver Evidencia_PE2.pdf)

| # | Captura | Criterio | Resultado |
|---|---|---|---|
| 1 | Kinesis stream detail (Provisioned, 2 shards, Active) | PE2-01 (30%) | OK |
| 2 | Productor: 50 eventos, partition keys variables (5 merchants) | PE2-01 | 50 enviados |
| 3 | CloudWatch Monitoring — IncomingRecords | PE2-04 (15%) | OK |
| 4 | Firehose Monitoring — Records read / Delivery success | PE2-02 (25%) | OK |
| 5 | Objetos S3 entregados por Firehose (bronze/YYYY/MM/DD/HH/) | PE2-02 | OK |

## Notas

- Stream `streaming-data-platform-events-dev` (el definido en el Terraform de PE2).
- Firehose con buffering 5 MB / 60 s según rúbrica.
- Partition key = merchant_id (variable) → distribución en los 2 shards.
- Limpieza completa confirmada.
