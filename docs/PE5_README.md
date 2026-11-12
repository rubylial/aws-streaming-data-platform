
**Mitigación en este proyecto**: con un solo job Flink escribiendo a cada tabla, los conflictos de concurrencia son poco probables. La configuración de reintentos protege contra race conditions durante compactación o mantenimiento.

### Recuperación ante fallos

| Escenario | Comportamiento |
|---|---|
| Flink crash antes de checkpoint | Eventos perdidos se re-procesan desde Kinesis (retención 24h) |
| Flink crash después de checkpoint | Restaura desde último checkpoint exitoso; commits Iceberg pendientes se descartan |
| Commit Iceberg falla | Reintento automático con optimistic concurrency |
| Archivos huérfanos (sin commit) | Lifecycle rule S3 expira noncurrent versions tras 30 días |

### Idempotencia

Se verifica con query Athena:

```sql
SELECT event_id, COUNT(*) AS occurrences
FROM streaming_data_platform_dev.transactions
GROUP BY event_id
HAVING COUNT(*) > 1;
-- Resultado esperado: 0 filas (sin duplicados)
```

## Consultas Athena

El archivo `sql/athena_queries.sql` incluye:

1. **Metadata**: `SHOW TABLES`, `DESCRIBE` para verificar catálogo
2. **Distribución por merchant**: count, sum, avg, rango temporal
3. **Eventos por status**: distribución de estados
4. **Duplicados**: verificación de idempotencia por `event_id`
5. **Agregaciones**: últimas ventanas y tendencias con `LAG()`
6. **Metadata Iceberg**: snapshots, manifests, files
7. **Time Travel**: consultas históricas por snapshot o timestamp

**Estado**: consultas preparadas y revisadas estáticamente. Ejecución real requiere Athena + Glue + S3 desplegados (`BLOQUEADO_POR_COSTO`).

## Terraform — Recursos declarados

| Recurso | Tipo | Propósito |
|---|---|---|
| S3 Bucket lakehouse | `aws_s3_bucket` | Almacenamiento Iceberg |
| S3 Versioning | `aws_s3_bucket_versioning` | Protección de datos |
| S3 Encryption | `aws_s3_bucket_server_side_encryption_configuration` | KMS |
| S3 Public Block | `aws_s3_bucket_public_access_block` | Seguridad |
| S3 Lifecycle | `aws_s3_bucket_lifecycle_configuration` | Expiración orphan files |
| Glue Database | `aws_glue_catalog_database` | Catálogo Iceberg |
| Glue Table transactions | `aws_glue_catalog_table` | Tabla Iceberg + partition_keys |
| Glue Table aggregations | `aws_glue_catalog_table` | Tabla Iceberg + partition_keys |
| IAM Glue → S3 | `aws_iam_role_policy` | Acceso lakehouse |
| IAM Glue → Catalog | `aws_iam_role_policy` | Operaciones catálogo |
| IAM Flink → S3 lakehouse | `aws_iam_role_policy` | Escritura Iceberg |
| IAM Flink → Glue Catalog | `aws_iam_role_policy` | Registro catálogo |
| IAM Flink → KMS | `aws_iam_role_policy` | Cifrado/descifrado |

### Instanciación del módulo

El módulo está instanciado en `terraform/environments/dev/main.tf`:

```hcl
module "lakehouse" {
  source          = "../../modules/lakehouse"
  project_name    = var.project_name
  environment     = var.environment
  aws_region      = var.aws_region
  kms_key_arn     = module.ingestion.kms_key_arn
  glue_role_name  = module.identity.glue_service_role_name
  flink_role_name = module.identity.flink_execution_role_name
  tags            = { Component = "lakehouse" }
}
```

## Dependencias Maven (PE5)

Agregadas al `flink-app/pom.xml`:

| Dependencia | Versión | Propósito |
|---|---|---|
| `iceberg-flink-runtime-1.19` | 1.5.2 | Bundle Iceberg para Flink 1.19 |
| `iceberg-aws` | 1.5.2 | GlueCatalog + S3FileIO |
| `flink-table-planner_2.12` | 1.19.0 | Ejecutar SQL/Table API en runtime |
| `flink-table-api-java` | 1.19.0 | API de tablas |
| `flink-table-api-java-bridge` | 1.19.0 | Bridge DataStream ↔ Table |
| `glue` (AWS SDK v2) | 2.25.11 | Cliente Glue para GlueCatalog |
| `s3` (AWS SDK v2) | 2.25.11 | Cliente S3 para S3FileIO |
| `sts` (AWS SDK v2) | 2.25.11 | Credenciales IAM role |

## Limitaciones académicas

| Aspecto | Estado |
|---|---|
| Terraform HCL válido | ✅ `terraform validate` en CI |
| Módulo instanciado en dev/main.tf | ✅ Con variables reales |
| Partition keys definidas | ✅ `merchant_id` con justificación |
| Dependencias Iceberg en pom.xml | ✅ Compilable |
| IcebergSinkBuilder cableado | ✅ `--sink.mode iceberg` en StreamingJob |
| Concurrencia documentada | ✅ Optimistic concurrency + reintentos |
| Consultas Athena preparadas | ✅ SQL revisado estáticamente |
| Creación efectiva tabla Iceberg en Glue | `BLOQUEADO_POR_COSTO` |
| Escritura Flink → Iceberg → S3 | `BLOQUEADO_POR_COSTO` |
| Consultas Athena ejecutadas | `BLOQUEADO_POR_COSTO` |
| Verificación metadata/snapshots Iceberg | `BLOQUEADO_POR_COSTO` |
| Time Travel verificado | `BLOQUEADO_POR_COSTO` |

## Criterios de rúbrica PE5

| Criterio | Peso | Estado |
|---|---|---|
| Glue Catalog, S3 y Terraform | 30% | `IMPLEMENTED_UNVERIFIED` — módulo completo con partition_keys, instanciado en dev |
| Sink Flink-Iceberg / conectores | 32% | `IMPLEMENTED_UNVERIFIED` — IcebergSinkBuilder cableado, deps en pom.xml, modo print/iceberg |
| Consistencia commits y recuperación | 18% | `IMPLEMENTED_UNVERIFIED` — checkpoints 60s, optimistic concurrency, reintentos documentados |
| Validación Athena / metadata | 12% | `BLOQUEADO_POR_COSTO` — queries preparadas, no ejecutadas |
| Documentación y evidencia | 8% | `IMPLEMENTED_UNVERIFIED` — README completo, pendiente evidencia AWS |
