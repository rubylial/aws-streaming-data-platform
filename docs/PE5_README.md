# PE5 — Lakehouse Iceberg con Glue Data Catalog

## Arquitectura

```
Managed Apache Flink (PE4)
        │
        ├─── IcebergSinkBuilder
        │         │
        │         ▼
        │    Glue Data Catalog
        │    ┌────────────────────────────┐
        │    │ Database:                   │
        │    │   streaming_data_platform_  │
        │    │   dev                       │
        │    │                             │
        │    │ Tables (Iceberg v2):        │
        │    │  ├── transactions           │
        │    │  └── merchant_aggregations  │
        │    └────────────────────────────┘
        │              │
        ▼              ▼
   S3 Lakehouse Bucket
   s3://<project>-lakehouse-dev/
   └── warehouse/
       ├── transactions/
       │   ├── metadata/    ← Iceberg metadata JSON
       │   └── data/        ← Parquet + Snappy
       └── merchant_aggregations/
           ├── metadata/
           └── data/
                  │
                  ▼
             Amazon Athena
             (consultas SQL sobre Iceberg)
                  │
                  ▼
             Amazon Redshift (PE6)
             (external schema FROM DATA CATALOG)
```

## Contrato de datos

### Tabla `transactions` (eventos individuales)

| Columna | Tipo | Origen |
|---|---|---|
| `event_id` | string | Evento original |
| `event_time` | timestamp | Event Time del productor |
| `merchant_id` | string | Partition key |
| `amount` | double | Monto de transacción |
| `currency` | string | Moneda |
| `status` | string | Estado (completed/pending/failed) |
| `processing_time` | timestamp | Hora de procesamiento Flink |
| `window_start` | timestamp | NULL para eventos individuales |
| `window_end` | timestamp | NULL para eventos individuales |

### Tabla `merchant_aggregations` (ventanas)

| Columna | Tipo | Origen |
|---|---|---|
| `merchant_id` | string | Key de agrupación |
| `window_start` | timestamp | Inicio de ventana |
| `window_end` | timestamp | Fin de ventana |
| `transaction_count` | bigint | Conteo por ventana |
| `total_amount` | double | Suma por ventana |
| `avg_amount` | double | Promedio por ventana |

## Estructura de archivos

```
terraform/modules/lakehouse/
  main.tf                     # S3 + Glue Database + Tablas Iceberg + IAM
  variables.tf                # Variables del módulo
  outputs.tf                  # Outputs del módulo
flink-app/src/main/java/com/streaming/flink/
  IcebergSinkBuilder.java     # Sink Flink → Iceberg vía Glue Catalog
sql/
  athena_queries.sql          # Consultas de validación Athena
docs/
  PE5_README.md               # Este archivo
```

## Componentes

### Glue Data Catalog como catálogo Iceberg

- **Database**: `streaming_data_platform_dev`
- **Tablas**: registradas con `open_table_format_input` Iceberg v2
- **Metadata**: operación `CREATE` — Glue inicializa el metadata tree
- **Formato**: Parquet con compresión Snappy

### Flink → Iceberg (IcebergSinkBuilder)

- Registra catálogo Flink SQL con `GlueCatalog`
- `S3FileIO` para I/O de archivos Iceberg
- Convierte `DataStream` a `Table` vía Table API
- `INSERT INTO` tabla Iceberg desde vista temporal
- Escribe tanto eventos individuales como agregaciones

### Dependencias Maven adicionales (pom.xml)

```xml
<!-- Agregar al pom.xml existente de PE4 -->
<dependency>
    <groupId>org.apache.iceberg</groupId>
    <artifactId>iceberg-flink-runtime-1.19</artifactId>
    <version>1.5.2</version>
</dependency>
<dependency>
    <groupId>software.amazon.awssdk</groupId>
    <artifactId>glue</artifactId>
    <version>2.25.0</version>
</dependency>
```

### Consistencia y recuperación

- **Commits atómicos**: Iceberg garantiza commits atómicos a nivel de tabla; cada escritura produce un snapshot consistente.
- **Checkpoint + Iceberg**: El sink Iceberg de Flink coordina commits con el mecanismo de checkpoints. Un commit Iceberg ocurre al completar un checkpoint exitoso.
- **Recuperación**: Al fallar y restaurar desde checkpoint, Flink re-procesa desde el último checkpoint exitoso. Los commits Iceberg pendientes (no confirmados) se descartan.
- **Idempotencia**: Se verifica con `SELECT event_id, COUNT(*) ... HAVING COUNT(*) > 1` en Athena.
- **Limitación**: La exacta-una-vez entre Flink y el sink Iceberg depende de la implementación del conector y la coordinación con checkpoints. No se garantiza automáticamente sin validación.

### Consultas Athena

El archivo `sql/athena_queries.sql` incluye:
1. Verificación de metadata (SHOW TABLES, DESCRIBE)
2. Distribución por merchant y status
3. Detección de duplicados
4. Tendencias con `LAG()` sobre ventanas
5. Metadata Iceberg (snapshots, manifests, files)
6. Time Travel (consulta histórica)

## Terraform — Recursos declarados

| Recurso | Tipo | Propósito |
|---|---|---|
| S3 Bucket lakehouse | `aws_s3_bucket` | Almacenamiento Iceberg |
| S3 Encryption | `aws_s3_bucket_server_side_encryption_configuration` | KMS |
| S3 Lifecycle | `aws_s3_bucket_lifecycle_configuration` | Expiración orphan files |
| Glue Database | `aws_glue_catalog_database` | Catálogo Iceberg |
| Glue Table transactions | `aws_glue_catalog_table` | Tabla Iceberg eventos |
| Glue Table aggregations | `aws_glue_catalog_table` | Tabla Iceberg ventanas |
| IAM Glue → S3 | `aws_iam_role_policy` | Acceso lakehouse |
| IAM Glue → Catalog | `aws_iam_role_policy` | Operaciones catálogo |
| IAM Flink → S3 lakehouse | `aws_iam_role_policy` | Escritura Iceberg |
| IAM Flink → Glue Catalog | `aws_iam_role_policy` | Registro catálogo |
| IAM Flink → KMS | `aws_iam_role_policy` | Cifrado/descifrado |

## Limitaciones académicas

| Aspecto | Estado |
|---|---|
| Terraform HCL válido | ✅ `terraform validate` en CI |
| IcebergSinkBuilder compilable | ✅ con dependencias en pom.xml |
| Consultas Athena preparadas | ✅ SQL revisado |
| Creación efectiva tabla Iceberg en Glue | `BLOQUEADO_POR_COSTO` |
| Escritura Flink → Iceberg → S3 | `BLOQUEADO_POR_COSTO` |
| Consultas Athena ejecutadas | `BLOQUEADO_POR_COSTO` |
| Verificación metadata/snapshots Iceberg | `BLOQUEADO_POR_COSTO` |
| Time Travel verificado | `BLOQUEADO_POR_COSTO` |
| Consistencia checkpoint-commit | `BLOQUEADO_POR_COSTO` |

## Criterios de rúbrica PE5

| Criterio | Peso | Estado |
|---|---|---|
| Glue Catalog, S3 y Terraform | 30% | `IMPLEMENTED_UNVERIFIED` |
| Sink Flink-Iceberg / conectores | 32% | `IMPLEMENTED_UNVERIFIED` |
| Consistencia commits y recuperación | 18% | `IMPLEMENTED_UNVERIFIED` |
| Validación Athena / metadata | 12% | `BLOQUEADO_POR_COSTO` |
| Documentación y evidencia | 8% | `IMPLEMENTED_UNVERIFIED` |
