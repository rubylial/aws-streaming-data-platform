# PROJECT STATUS — AWS Real-Time Data Engineering Capstone

**Última actualización:** 2026-11-12  
**Repositorio:** github.com/rubylial/aws-streaming-data-platform  
**Branch principal:** main

## Estado general

| Entrega | Estado | CI | Evidencia AWS | Branch |
|---|---|---|---|---|
| PE1 — Terraform/VPC/IAM/Backend | `LOCAL_VALIDATED` | ✅ | ❌ | `main` |
| PE2 — Kinesis/Firehose/Productor | `LOCAL_VALIDATED` | ✅ | ❌ | `feature/ingesta-kinesis` → `main` |
| PE3 — Kafka/Kubernetes/Spark | `LOCAL_VALIDATED` | ✅ | N/A (local) | `feature/pe3-kafka-spark` → `main` |
| PE4 — Flink Stateful | `LOCAL_VALIDATED` | ✅ | ❌ | `feature/pe4-flink` → `main` |
| PE5 — Iceberg/Glue/Athena | `AWS_VALIDATED` | ✅ | ✅ | `feature/pe5-iceberg` → `main` |
| PE6 — Redshift Streaming | `IMPLEMENTED_UNVERIFIED` | ✅ | ❌ | `feature/pe6-redshift` → `main` |
| DAAT — Documento final | `LOCAL_VALIDATED` | — | Parcial | `main` |

## Restricción de costo

**Presupuesto: $0 USD.** Validación AWS realizada exclusivamente en AWS Academy Learner Lab con créditos académicos precargados ($50 pool).

### Validaciones realizadas

**Sin costo (todas las entregas):**
- `terraform fmt -check`, `terraform init -backend=false`, `terraform validate` (CI)
- Compilación Maven y tests JUnit (Flink)
- Revisión estática de SQL (Redshift, Athena)
- Docker Compose local (PE3 Kafka/Spark)

**AWS Academy Learner Lab (2026-11-12, ~$0.05):**
- PE5: Glue database `streaming_data_platform_dev` creada
- PE5: Tablas Iceberg `transactions` y `merchant_aggregations` creadas en Glue Catalog
- PE5: Athena — SHOW TABLES, DESCRIBE ×2, SELECT COUNT(*) ejecutados exitosamente
- PE5: Limpieza completa de recursos realizada
- Créditos restantes: ~$49.55 de $50

## PE5 — Evidencias AWS (2026-11-12)

| Evidencia | Resultado | Captura |
|---|---|---|
| Glue database creada | `streaming_data_platform_dev` | ✅ Screenshot |
| Tabla `transactions` (Iceberg, 7 columnas + partition) | Creada con format Apache Iceberg | ✅ Screenshot |
| Tabla `merchant_aggregations` (Iceberg, 6 columnas + partition) | Creada con format Apache Iceberg | ✅ Screenshot |
| Athena: SHOW TABLES | 2 tablas listadas | ✅ Screenshot |
| Athena: DESCRIBE transactions | Schema correcto | ✅ Screenshot |
| Athena: DESCRIBE merchant_aggregations | Schema correcto | ✅ Screenshot |
| Athena: SELECT COUNT(*) | `total_events = 0` (esperado, sin Flink) | ✅ Screenshot |
| Athena: Recent queries | Historial de ejecución | ✅ Screenshot |
| Limpieza completa | Tablas, database y buckets S3 borrados | ✅ Confirmado |

> **Nota:** Las tablas están vacías porque Flink no fue ejecutado en AWS.
> Esto demuestra que Glue Catalog + Athena están correctamente configurados
> para recibir y consultar datos Iceberg. La escritura fue validada localmente
> con IcebergLocalTest (BUILD SUCCESS).

## Criterios BLOQUEADOS

Los siguientes criterios requieren despliegue AWS real y están marcados `BLOQUEADO_POR_COSTO`:
- PE1: VPC/subnets desplegadas en AWS
- PE2: Stream activo, objetos S3 verificados
- PE4: Ejecución Flink en Managed Service, checkpoints observados
- ~~PE5: Tablas Iceberg con datos reales, queries Athena ejecutadas~~ → **RESUELTO 2026-11-12**
- PE6: Redshift Serverless activo, MV con datos, JOINs verificados
- DAAT: Prueba end-to-end (30% del peso)

## Siguiente paso

1. **PE6 declarativa**: preparar SQL, Terraform, IAM y README sin costo
2. **PE6 lab**: evaluar si los créditos restantes (~$49.55) permiten una sesión Redshift (~$4-8)
3. **DAAT**: consolidar evidencias, arquitectura y limitaciones
