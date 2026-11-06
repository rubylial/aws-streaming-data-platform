# PROJECT STATUS — AWS Real-Time Data Engineering Capstone

**Última actualización:** 2026-11-04  
**Repositorio:** github.com/rubylial/aws-streaming-data-platform  
**Branch principal:** main

## Estado general

| Entrega | Estado | CI | Branch |
|---|---|---|---|
| PE1 — Terraform/VPC/IAM/Backend | `LOCAL_VALIDATED` | ✅ | `main` |
| PE2 — Kinesis/Firehose/Productor | `LOCAL_VALIDATED` | ✅ | `feature/ingesta-kinesis` → `main` |
| PE3 — Kafka/Kubernetes/Spark | `LOCAL_VALIDATED` | ✅ | `feature/pe3-kafka-spark` → `main` |
| PE4 — Flink Stateful | `LOCAL_VALIDATED` | ✅ | `feature/pe4-flink` → `main` |
| PE5 — Iceberg/Glue/Athena | `LOCAL_VALIDATED` | ✅ | `feature/pe5-iceberg` → `main` |
| PE6 — Redshift Streaming | `IMPLEMENTED_UNVERIFIED` | ✅ | `feature/pe6-redshift` → `main` |
| DAAT — Documento final | `LOCAL_VALIDATED` | — | `main` |

## Restricción de costo

**Presupuesto: $0 USD.** Ningún recurso AWS fue provisionado. Todo el código fue validado con:
- `terraform fmt -check`, `terraform init -backend=false`, `terraform validate` (CI)
- Compilación Maven y tests JUnit (Flink)
- Revisión estática de SQL (Redshift, Athena)
- Docker Compose local (PE3 Kafka/Spark)

## Criterios BLOQUEADOS

Los siguientes criterios requieren despliegue AWS real y están marcados `BLOQUEADO_POR_COSTO`:
- PE1: VPC/subnets desplegadas en AWS
- PE2: Stream activo, objetos S3 verificados
- PE4: Ejecución Flink en Managed Service, checkpoints observados
- PE5: Tablas Iceberg con datos reales, queries Athena ejecutadas
- PE6: Redshift Serverless activo, MV con datos, JOINs verificados
- DAAT: Prueba end-to-end (30% del peso)

## Siguiente paso

Consultar al docente sobre aceptación de evidencias de simulación/validación estática como alternativa al despliegue AWS real con costo.
