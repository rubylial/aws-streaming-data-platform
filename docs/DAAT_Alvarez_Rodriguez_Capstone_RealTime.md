# Documento de Arquitectura y Auditoría Técnica (DAAT)

## Plataforma de Streaming de Datos en Tiempo Real sobre AWS

**Autora:** Ruby Lileth Alvarez Rodríguez  
**Fecha:** 4 de noviembre de 2026  
**Repositorio:** [github.com/rubylial/aws-streaming-data-platform](https://github.com/rubylial/aws-streaming-data-platform)  
**Branch principal:** `main`  
**Estado:** CÓDIGO COMPLETO — DESPLIEGUE AWS `BLOQUEADO_POR_COSTO`

---

## Resumen ejecutivo

Este documento presenta el diseño, implementación y auditoría técnica de una plataforma de streaming de datos en tiempo real sobre Amazon Web Services (AWS). El sistema procesa transacciones financieras desde un productor JSON a través de tres rutas de consumo paralelas: almacenamiento raw en S3 vía Firehose (Bronze), procesamiento stateful con Apache Flink hacia un lakehouse Apache Iceberg consultable con Athena, y streaming ingestion directa en Amazon Redshift mediante Materialized Views.

**Alcance verificado:**
- Código Terraform modular completo (6 módulos: bootstrap, networking, identity, ingestion, analytics, lakehouse, redshift)
- Aplicación Apache Flink 1.19 en Java con Event Time, Watermarks, ventanas Tumbling/Sliding, state y checkpoints
- Sink Iceberg vía Flink SQL con GlueCatalog y S3FileIO
- SQL Redshift: streaming ingestion FROM KINESIS, consultas Iceberg FROM DATA CATALOG, JOINs analíticos
- Ejercicio paralelo Kafka/Kubernetes/Spark (PE3) independiente de la arquitectura AWS
- CI/CD con GitHub Actions: `terraform fmt`, `validate`, `init -backend=false`
- Tests unitarios Java (JUnit 4) para la aplicación Flink

**Limitaciones académicas:**
- Restricción de costo $0 USD: ningún recurso AWS fue provisionado. Todo el código se validó estáticamente (CI) y localmente (compilación Maven, tests unitarios). Los criterios de rúbrica que exigen despliegue real están marcados `BLOQUEADO_POR_COSTO`.
- No se generaron métricas reales de latencia, throughput ni CloudWatch.
- No se ejecutaron queries reales en Athena ni Redshift.

---

## 1. Alcance, requerimientos y rúbricas

### 1.1 Preentregas

| PE | Tema | Branch | Estado |
|---|---|---|---|
| PE1 | Terraform + VPC + IAM + Backend | `main` | `LOCAL_VALIDATED` (CI ✅) |
| PE2 | Kinesis + Firehose + Productor | `feature/ingesta-kinesis` → `main` | `LOCAL_VALIDATED` (CI ✅) |
| PE3 | Kafka + Kubernetes + Spark | `feature/pe3-kafka-spark` → `main` | `LOCAL_VALIDATED` |
| PE4 | Flink Stateful | `feature/pe4-flink` → `main` | `LOCAL_VALIDATED` (CI ✅, tests ✅) |
| PE5 | Iceberg + Glue + Athena | `feature/pe5-iceberg` → `main` | `LOCAL_VALIDATED` (CI ✅) |
| PE6 | Redshift Streaming + Iceberg | `feature/pe6-redshift` → `main` | `IMPLEMENTED_UNVERIFIED` (CI ✅) |

### 1.2 Ponderaciones de rúbrica

Todas las ponderaciones provienen de enunciados institucionales compartidos por la autora. Los pesos de PE3 (30/20/25/15/10) fueron recuperados del texto original.

---

## 2. Diagrama integral y descripción de flujos

### 2.1 Arquitectura AWS — Flujo principal

```
┌─────────────────────────────────────────────────────────────────────┐
│                        VPC PRIVADA (Multi-AZ)                       │
│  ┌─────────────────┐                                                │
│  │  Productor JSON  │                                                │
│  │  (Python SDK)    │                                                │
│  └────────┬─────────┘                                                │
│           │ PutRecord (merchant_id como PartitionKey)               │
│           ▼                                                          │
│  ┌─────────────────────────────────────────────┐                    │
│  │     KINESIS DATA STREAMS (2 shards)          │                    │
│  │     Cifrado KMS — Retención 24h              │                    │
│  └──────┬──────────────┬──────────────┬─────────┘                    │
│         │              │              │                               │
│    ┌────▼────┐   ┌─────▼─────┐  ┌────▼──────────────┐              │
│    │FIREHOSE │   │MANAGED    │  │REDSHIFT SERVERLESS │              │
│    │         │   │FLINK 1.19 │  │                    │              │
│    │Buffer:  │   │           │  │External Schema     │              │
│    │5MB/60s  │   │Watermarks │  │FROM KINESIS        │              │
│    │         │   │Tumbling   │  │                    │              │
│    │         │   │Sliding    │  │MV: 6 campos JSON   │              │
│    │         │   │Checkpoints│  │AUTO REFRESH YES    │              │
│    └────┬────┘   │60s        │  │                    │              │
│         │        └─────┬─────┘  │External Schema     │              │
│         ▼              │        │FROM DATA CATALOG   │              │
│    ┌─────────┐         ▼        │                    │              │
│    │S3 BRONZE│   ┌───────────┐  │JOIN streaming ×    │              │
│    │(raw/    │   │S3 LAKEHOUSE│  │    Iceberg         │              │
│    │auditoría│   │  ICEBERG  │  └────────────────────┘              │
│    └─────────┘   │  v2       │                                      │
│                  └─────┬─────┘                                      │
│                        │                                            │
│                  ┌─────▼─────┐                                      │
│                  │GLUE DATA  │                                      │
│                  │CATALOG    │                                      │
│                  └─────┬─────┘                                      │
│                        │                                            │
│                  ┌─────▼─────┐                                      │
│                  │  ATHENA   │                                      │
│                  │(Iceberg   │                                      │
│                  │ queries)  │                                      │
│                  └───────────┘                                      │
│                                                                     │
│  Transversal: IAM mínimo privilegio · KMS · CloudWatch · S3 Gateway│
└─────────────────────────────────────────────────────────────────────┘
```

### 2.2 Tres rutas de consumo

| Ruta | Propósito | Latencia esperada | Consumidor |
|---|---|---|---|
| **Bronze** (Firehose → S3) | Almacenamiento raw, auditoría, replay | Segundos–minutos (buffer 5MB/60s) | Firehose delivery stream |
| **Lakehouse** (Flink → Iceberg) | Procesamiento stateful, agregaciones, time travel | Segundos (checkpoint 60s) | Managed Apache Flink |
| **Analítica** (Redshift Streaming) | Consultas SQL en tiempo real, JOINs | Segundos–minutos (AUTO REFRESH) | Redshift Serverless |

### 2.3 PE3 — Ejercicio paralelo (NO integrado con AWS)

```
Docker Compose / Kind
  └── Kafka (3+ particiones, topic: urban_sensors)
       ├── Productor Python (sensor_id, temperature, humidity, air_quality_index, timestamp)
       └── Spark Structured Streaming (ventana 1 min, promedios por sensor)
```

---

## 3. Diseño Terraform y despliegue reproducible

### 3.1 Estructura de módulos

```
terraform/
├── bootstrap/              # S3 bucket + DynamoDB table para remote state
│   ├── main.tf
│   ├── variables.tf
│   └── outputs.tf
├── modules/
│   ├── networking/          # VPC, subnets, S3 Gateway Endpoint, route tables
│   ├── identity/            # IAM roles y policies por servicio
│   ├── ingestion/           # Kinesis Data Streams, Firehose, KMS
│   ├── analytics/           # Managed Flink, S3 artifacts, CloudWatch
│   ├── lakehouse/           # S3 lakehouse, Glue Catalog, tablas Iceberg
│   └── redshift/            # Redshift Serverless, IAM Kinesis/Glue/S3/KMS
└── environments/
    └── dev/
        ├── main.tf          # Root module: composición de módulos
        ├── variables.tf
        ├── outputs.tf
        ├── terraform.tfvars.example
        └── backend.tf       # S3 + DynamoDB backend
```

### 3.2 Backend remoto

- **S3 bucket** con versionado, cifrado AES-256 y bloqueo de acceso público
- **DynamoDB table** para state locking (`LockID` como hash key)
- Bootstrap separado del root module para evitar dependencia circular
- Instrucciones de despliegue desde entorno limpio en `docs/PE1_README.md`

### 3.3 CI/CD — GitHub Actions

```yaml
# .github/workflows/terraform-validate.yml
# Ejecuta en cada push y PR:
# 1. terraform fmt -check -recursive -diff
# 2. terraform init -backend=false
# 3. terraform validate
# NO ejecuta: apply, destroy, plan contra AWS
```

### 3.4 Despliegue reproducible (runbook teórico)

1. Clonar repositorio y configurar credenciales AWS (OIDC preferible)
2. Ejecutar bootstrap: `cd terraform/bootstrap && terraform init && terraform apply`
3. Compilar Flink: `cd flink-app && mvn clean package -DskipTests`
4. Subir JAR a S3 artifacts bucket
5. Init + plan + apply del root module: `cd terraform/environments/dev && terraform init && terraform plan -out=tfplan && terraform apply tfplan`
6. Ejecutar SQL Redshift en orden (01→05)
7. Iniciar productor Python con eventos sintéticos
8. Verificar: CloudWatch, S3 Bronze, Athena, Redshift MV

**Nota:** Este runbook NO se ejecutó. Es el plan de despliegue documentado para validación futura.

---

## 4. Contrato de eventos

### 4.1 Esquema JSON

```json
{
  "event_id": "uuid-v4",
  "event_time": "2026-11-04T06:00:00Z",
  "merchant_id": "merchant_001",
  "amount": 150.75,
  "currency": "USD",
  "status": "completed"
}
```

### 4.2 Campos y tipos

| Campo | Tipo | Descripción | Obligatorio |
|---|---|---|---|
| `event_id` | `string` (UUID v4) | Identificador único de evento | ✅ |
| `event_time` | `string` (ISO 8601 UTC) | Timestamp del evento en origen | ✅ |
| `merchant_id` | `string` | Identificador del merchant | ✅ |
| `amount` | `number` (decimal) | Monto de la transacción | ✅ |
| `currency` | `string` | Moneda (ISO 4217) | ✅ |
| `status` | `string` | Estado: `completed`, `pending`, `failed` | ✅ |

### 4.3 PartitionKey

`merchant_id` — distribuye shards por merchant para balancear carga y permitir agregaciones por merchant en Flink.

### 4.4 Políticas de manejo

- **Duplicados:** `event_id` permite deduplicación en cada consumidor
- **Eventos tardíos:** Watermarks con `BoundedOutOfOrderness(5s)` + `withIdleness(30s)` en Flink
- **Eventos malformados:** `TransactionDeserializer` retorna `null` con log de warning; Flink los descarta
- **Schema evolution:** no implementada en esta versión; requeriría Schema Registry

---

## 5. Kinesis y Firehose

### 5.1 Kinesis Data Streams

- **Modo:** PROVISIONED, 2 shards
- **Capacidad:** 2 MB/s escritura, 4 MB/s lectura
- **Retención:** 24 horas (default)
- **Cifrado:** KMS (llave dedicada del proyecto)
- **Enhanced Fan-Out:** no habilitado (3 consumidores: Firehose, Flink, Redshift)
- **Métricas CloudWatch:** habilitadas a nivel de shard

### 5.2 Firehose Delivery Stream

- **Origen:** Kinesis Data Stream
- **Destino:** S3 (Bronze)
- **Buffering:** 5 MB / 60 segundos (mínimo de ambos dispara la entrega)
- **Prefijo S3:** `raw/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/`
- **Error prefix:** `errors/`
- **Cifrado:** SSE-S3
- **Compresión:** no habilitada (raw JSON para auditoría)

### 5.3 Productor Python

- `producer/kinesis_producer.py`: genera eventos sintéticos con `boto3.put_record()`
- Partition key: `merchant_id`
- Pool de 5 merchants, distribución aleatoria
- Configurable vía variables de entorno

---

## 6. Flink

### 6.1 Arquitectura de la aplicación

```
flink-app/
├── pom.xml                           # Maven, Flink 1.19, shade plugin
└── src/
    ├── main/java/com/streaming/flink/
    │   ├── StreamingJob.java         # Job principal: source, watermarks, ventanas, sink
    │   ├── TransactionEvent.java     # POJO con @JsonProperty
    │   ├── TransactionDeserializer.java # Deserialización JSON con manejo de errores
    │   ├── MerchantAggregation.java  # POJO resultado de ventanas
    │   └── IcebergSinkBuilder.java   # Sink Flink SQL → Iceberg vía GlueCatalog
    └── test/java/com/streaming/flink/
        └── TransactionEventTest.java # 7 tests unitarios (JUnit 4)
```

### 6.2 Event Time y Watermarks

```java
WatermarkStrategy
    .<TransactionEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
    .withTimestampAssigner((event, ts) -> event.getEventTimeMillis())
    .withIdleness(Duration.ofSeconds(30));
```

- **BoundedOutOfOrderness:** 5 segundos — tolera reordenamiento moderado
- **Idleness:** 30 segundos — avanza watermarks en particiones sin datos
- **Timestamp assigner:** `TransactionEvent.getEventTimeMillis()` (ISO 8601 → epoch millis)

### 6.3 Ventanas

| Tipo | Tamaño | Slide | Propósito |
|---|---|---|---|
| TumblingEventTimeWindows | 1 minuto | — | Agregaciones por merchant no superpuestas |
| SlidingEventTimeWindows | 5 minutos | 1 minuto | Tendencias suavizadas con overlap |

### 6.4 Agregaciones

- **AggregateFunction** (`CountSumAggregate`): acumulador incremental `(count, sum)` — O(1) memoria
- **ProcessWindowFunction** (`MerchantWindowFunction`): acceso a metadata de ventana (start, end), calcula `avg = sum / count`, emite `MerchantAggregation`

### 6.5 Checkpoints y tolerancia a fallos

```java
env.enableCheckpointing(60_000);  // cada 60 segundos
config.setMinPauseBetweenCheckpoints(30_000);
config.setCheckpointTimeout(120_000);
config.setMaxConcurrentCheckpoints(1);
config.setTolerableCheckpointFailureNumber(3);
```

**Nota importante:** Los checkpoints de Flink garantizan consistencia del estado interno del job. Sin embargo, **no garantizan automáticamente exactly-once** hacia sinks externos: eso depende del protocolo del sink (e.g., Iceberg atomic commits, idempotent writes). En Managed Flink, los checkpoints son administrados por el servicio; los snapshots son copias persistentes independientes del ciclo de vida del job.

### 6.6 Terraform — Managed Flink

- `aws_kinesisanalyticsv2_application` con runtime `FLINK-1_19`
- Configuración CUSTOM de checkpoints (60s interval, 30s pause)
- Parallelism: 2, auto-scaling deshabilitado
- CloudWatch: log group + log stream dedicados
- IAM: lectura Kinesis, acceso S3 artifacts, escritura CloudWatch

---

## 7. Iceberg, Glue y Athena

### 7.1 Glue Data Catalog como catálogo Iceberg

- Base de datos: `streaming_data_platform_dev`
- Dos tablas Iceberg v2 con `open_table_format_input { iceberg_input { ... } }`
- Metadata location en S3 lakehouse bucket

### 7.2 Tablas Iceberg

| Tabla | Campos | Propósito |
|---|---|---|
| `transactions` | event_id, event_time, merchant_id, amount, currency, status, processing_time, window_start, window_end | Eventos procesados por Flink |
| `merchant_aggregations` | merchant_id, window_start, window_end, transaction_count, total_amount, avg_amount | Agregaciones por ventana |

### 7.3 Sink Flink → Iceberg

`IcebergSinkBuilder.java` usa Flink SQL:
1. Registra catálogo Iceberg con `CREATE CATALOG` (GlueCatalog, S3FileIO, región)
2. Convierte `DataStream` a `Table` vía `StreamTableEnvironment.fromDataStream()`
3. `INSERT INTO` tablas Iceberg desde vistas temporales

**Dependencias adicionales (Maven):**
- `iceberg-flink-runtime-1.19:1.5.2`
- `software.amazon.awssdk:glue:2.25.0`

### 7.4 S3 Lakehouse

- Bucket con versionado, bloqueo de acceso público, cifrado KMS
- Lifecycle rule: limpieza de orphan files después de 30 días
- Estructura: `warehouse/{table_name}/data/` y `warehouse/{table_name}/metadata/`

### 7.5 Consistencia y recuperación

- **Atomic commits:** Iceberg garantiza commits atómicos por tabla — lectores ven snapshots completos
- **Checkpoint coordination:** Flink escribe a Iceberg durante checkpoint barriers, asegurando que solo datos checkpointed se commiten
- **Recuperación:** en caso de fallo, Flink retrocede al último checkpoint y el commit Iceberg incompleto no es visible para lectores
- **Limitación:** no hay transacción atómica entre las dos tablas (transactions y merchant_aggregations son commits independientes)

### 7.6 Consultas Athena

15+ queries documentadas en `sql/athena_queries.sql`:
- Exploración básica (SHOW TABLES, DESCRIBE, COUNT)
- Distribución por merchant y status
- Detección de duplicados por `event_id`
- Tendencias con LAG() window function
- Metadata Iceberg: `$snapshots`, `$manifests`, `$files`
- Time travel: `FOR VERSION AS OF`, `FOR TIMESTAMP AS OF`

---

## 8. Redshift

### 8.1 Dos esquemas externos — mecanismos distintos

| Schema | Tipo | Fuente | Propósito |
|---|---|---|---|
| `kinesis_streaming` | `FROM KINESIS` | Kinesis Data Stream directo | Streaming Ingestion en tiempo real |
| `lakehouse` | `FROM DATA CATALOG` | Glue Data Catalog (Iceberg) | Consulta de datos históricos (Spectrum) |

**Nota crítica:** `FROM KINESIS` y `FROM DATA CATALOG` son caminos completamente distintos. No se debe usar `FROM DATA CATALOG` para acceder al stream; `FROM KINESIS` lee el stream directamente, `FROM DATA CATALOG` consulta tablas registradas en el catálogo Glue.

### 8.2 Materialized View

```sql
CREATE MATERIALIZED VIEW mv_kinesis_transactions
AUTO REFRESH YES
AS
SELECT
    approximate_arrival_timestamp,
    partition_key, shard_id, sequence_number,
    -- 6 campos JSON (rúbrica exige ≥3)
    JSON_EXTRACT_PATH_TEXT(from_varbyte(kinesis_data, 'utf-8'), 'event_id')    AS event_id,
    CAST(JSON_EXTRACT_PATH_TEXT(..., 'event_time') AS TIMESTAMP)               AS event_time,
    JSON_EXTRACT_PATH_TEXT(..., 'merchant_id')                                 AS merchant_id,
    CAST(JSON_EXTRACT_PATH_TEXT(..., 'amount') AS DECIMAL(18,2))               AS amount,
    JSON_EXTRACT_PATH_TEXT(..., 'currency')                                    AS currency,
    JSON_EXTRACT_PATH_TEXT(..., 'status')                                      AS status
FROM kinesis_streaming."streaming-data-platform-events-dev";
```

### 8.3 JOIN streaming × Iceberg

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

Permite detectar anomalías comparando cada transacción en tiempo real con el promedio histórico de su merchant.

### 8.4 Refresh y monitoreo

- **AUTO REFRESH YES:** Redshift decide la frecuencia de refresh
- **REFRESH MANUAL:** `REFRESH MATERIALIZED VIEW mv_kinesis_transactions`
- **Métricas:** `SYS_MV_STATE`, `SYS_MV_REFRESH_HISTORY`, latencia event_time→arrival, freshness
- **Nota:** AUTO REFRESH no es instantáneo; existe lag entre la llegada al stream y la visibilidad en la MV

### 8.5 Terraform — Redshift Serverless

- Namespace + Workgroup (8 RPU base)
- Security Group: puerto 5439 desde VPC CIDR
- 4 políticas IAM: Kinesis (streaming), Glue (Spectrum), S3 (lakehouse), KMS (descifrado)

---

## 9. Seguridad

### 9.1 IAM — Mínimo privilegio

| Rol | Permisos principales | Restricciones |
|---|---|---|
| Firehose Delivery | `kinesis:Get*`, `s3:PutObject`, `kms:Decrypt` | Solo stream + bucket específicos |
| Flink Execution | `kinesis:Get*`, `s3:GetObject/PutObject` (lakehouse), `glue:Get*`, `kms:*` | Solo recursos del proyecto |
| Glue Service | `s3:Get*/List*` (lakehouse), `glue:*` (solo DB propia) | Sin acceso a otros catálogos |
| Redshift Service | `kinesis:Get*`, `glue:Get*`, `s3:Get*/List*` (lakehouse), `kms:Decrypt` | Solo lectura, sin escritura externa |
| CI/CD (GitHub Actions) | Terraform state read/write, `plan` | OIDC preferible; **nunca** admin global |

### 9.2 Cifrado

- **Kinesis:** KMS (llave dedicada del proyecto)
- **S3 (Bronze):** SSE-S3
- **S3 (Lakehouse):** SSE-KMS con bucket key enabled
- **Terraform state:** AES-256

### 9.3 Networking

- VPC privada con 2+ subnets en múltiples AZs
- S3 Gateway Endpoint: tráfico a S3 sin salir a internet
- Security groups restrictivos por servicio
- Sin NAT Gateway (optimización de costos)

### 9.4 Secretos y credenciales

- `terraform.tfvars.example` con valores ficticios
- `.gitignore`: `*.tfvars`, `*.tfstate*`, `.terraform/`
- CI/CD: variables de entorno o OIDC, nunca en código
- Contraseñas Redshift: `sensitive = true` en variables Terraform

---

## 10. Validación E2E y reconciliación

### 10.1 Metodología propuesta (no ejecutada)

1. Producir N eventos sintéticos con `event_id` rastreables
2. Verificar en S3 Bronze: N objetos con contenido JSON intacto
3. Verificar en Athena/Iceberg: N registros en `transactions`, agregaciones en `merchant_aggregations`
4. Verificar en Redshift MV: N registros con campos correctos
5. Comparar: `event_id` presente en las tres rutas, timestamps coherentes, montos sumados correctamente

### 10.2 Reconciliación entre rutas

```sql
-- En Redshift: comparar streaming vs Iceberg por merchant
SELECT
    COALESCE(s.merchant_id, i.merchant_id) AS merchant_id,
    s.streaming_count,
    i.iceberg_count,
    s.streaming_count - COALESCE(i.iceberg_count, 0) AS count_delta
FROM (subquery MV) s
FULL OUTER JOIN (subquery Iceberg) i
ON s.merchant_id = i.merchant_id;
```

### 10.3 Estado de validación

| Ruta | Validación local | Validación AWS |
|---|---|---|
| Productor → Kinesis | Contrato JSON ✅, mock SDK ✅ | `BLOQUEADO_POR_COSTO` |
| Kinesis → Firehose → S3 | Configuración Terraform ✅ | `BLOQUEADO_POR_COSTO` |
| Kinesis → Flink | Compilación + tests JUnit ✅ | `BLOQUEADO_POR_COSTO` |
| Flink → Iceberg | IcebergSinkBuilder compilable ✅ | `BLOQUEADO_POR_COSTO` |
| Iceberg → Athena | SQL revisado estáticamente ✅ | `BLOQUEADO_POR_COSTO` |
| Kinesis → Redshift MV | SQL revisado estáticamente ✅ | `BLOQUEADO_POR_COSTO` |
| Redshift MV ↔ Iceberg JOIN | SQL revisado estáticamente ✅ | `BLOQUEADO_POR_COSTO` |

---

## 11. Pruebas de falla, backpressure y recuperación

### 11.1 Fallas modeladas

| Escenario | Comportamiento esperado | Validado |
|---|---|---|
| Flink job crash | Reinicio desde último checkpoint (60s atrás), reproceso de eventos | Diseñado, no probado en AWS |
| Evento malformado | TransactionDeserializer retorna null, Flink descarta, warning en log | ✅ Test unitario |
| Evento tardío (>5s OOO) | Descartado por watermark | Diseñado, no probado en AWS |
| Kinesis throttling | IteratorAge aumenta, Flink backpressure, eventos se acumulan en retención | Diseñado, no probado |
| Checkpoint failure | Hasta 3 fallos tolerados antes de fallo del job | Configurado, no probado |
| Iceberg commit failure | Flink retrocede al checkpoint anterior, retry automático | Diseñado, no probado |

### 11.2 Backpressure y observabilidad

- **Flink:** checkpoints 60s, monitoreo vía CloudWatch log group/stream
- **Kinesis:** IteratorAge, ReadProvisionedThroughputExceeded, WriteProvisionedThroughputExceeded
- **Firehose:** DeliveryToS3.DataFreshness, DeliveryToS3.Records
- **Redshift MV:** SYS_MV_STATE (is_stale), SYS_MV_REFRESH_HISTORY (duration, rows)

### 11.3 Independencia de consumidores

**Los tres consumidores (Firehose, Flink, Redshift) son independientes:** cada uno lee del stream con su propio iterator. Redshift **no transmite backpressure a Flink ni a Kinesis**. Si Redshift se atrasa, su MV queda stale pero no afecta a los otros consumidores. La única restricción compartida es el throughput total del stream (2 shards × 2MB/s lectura por consumidor, o Enhanced Fan-Out si se habilita).

---

## 12. Costos y trade-offs

### 12.1 Estimación de costos (us-east-1, noviembre 2026)

| Servicio | Costo estimado/hora | Notas |
|---|---|---|
| Kinesis Data Streams (2 shards) | ~$0.03 | $0.015/shard/hora |
| Firehose | ~$0.035/GB | Según volumen ingerido |
| Managed Flink (2 KPU) | ~$0.22 | $0.11/KPU/hora |
| S3 | ~$0.023/GB/mes | Almacenamiento + requests |
| Glue Catalog | Gratis primeros 1M objetos | Después $1/100K objetos/mes |
| Athena | $5/TB escaneado | Por query |
| Redshift Serverless (8 RPU) | ~$2.88 | $0.36/RPU/hora, solo cuando corre |
| NAT Gateway | ~$0.045/hora + $0.045/GB | **Evitado** con S3 Gateway Endpoint |

### 12.2 Costo mensual estimado (operación continua)

| Escenario | Estimación | Notas |
|---|---|---|
| Mínimo (2 shards, 2 KPU, 8 RPU, uso bajo) | ~$250–400 USD/mes | Sin NAT, sin Enhanced Fan-Out |
| Con NAT Gateway | +$35–50 USD/mes | Solo el gateway, sin tráfico |
| Producción (más shards, más KPU) | $500+ USD/mes | Según volumen |

### 12.3 Trade-offs arquitectónicos

| Decisión | Ventaja | Desventaja |
|---|---|---|
| 3 consumidores independientes | Desacoplamiento, resiliencia | Mayor costo, posible inconsistencia temporal |
| Kinesis PROVISIONED vs ON-DEMAND | Costos predecibles | Gestión manual de shards |
| Sin NAT Gateway | Ahorro ~$35/mes | Solo S3 accesible vía endpoint; otros servicios requieren Interface Endpoints adicionales |
| Iceberg v2 | Time travel, atomic commits | Complejidad adicional, compactación necesaria |
| Redshift Serverless vs Provisioned | Pay-per-use | Latencia de cold start |
| AUTO REFRESH YES | Simplicidad | Sin control granular del timing |

---

## 13. Matriz de aceptación y auditoría

### PE1 — Terraform y seguridad (100% peso)

| Criterio | Peso | Estado | Evidencia |
|---|---|---|---|
| Arquitectura modular Terraform | 22% | `LOCAL_VALIDATED` | 6 módulos + root module, CI ✅ |
| VPC privada multi-AZ + S3 Endpoint | 28% | `LOCAL_VALIDATED` | HCL validate ✅, plan no ejecutado |
| IAM mínimo privilegio | 22% | `LOCAL_VALIDATED` | 5 roles con policies restrictivas |
| Backend S3 cifrado + locking | 18% | `LOCAL_VALIDATED` | Bootstrap module, DynamoDB lock |
| HCL/README/PLAN_OUTPUT | 10% | `LOCAL_VALIDATED` | README ✅, PLAN_OUTPUT pendiente AWS |

### PE2 — Kinesis + Firehose (100% peso)

| Criterio | Peso | Estado | Evidencia |
|---|---|---|---|
| Kinesis 2+ shards + productor JSON | 30% | `LOCAL_VALIDATED` | Terraform + producer.py, CI ✅ |
| Firehose → S3 (5MB/60s) | 25% | `LOCAL_VALIDATED` | HCL config, CI ✅ |
| IAM/KMS | 20% | `LOCAL_VALIDATED` | Policies + KMS key, CI ✅ |
| Pruebas/monitoreo/docs | 15% | `LOCAL_VALIDATED` | README + mock tests |
| Git branch | 10% | ✅ | `feature/ingesta-kinesis` merged |

### PE3 — Kafka/Kubernetes/Spark (100% peso)

| Criterio | Peso | Estado | Evidencia |
|---|---|---|---|
| Kubernetes YAML | 30% | `LOCAL_VALIDATED` | `/k8s/` Deployments + Services |
| Kafka topic + productor | 20% | `LOCAL_VALIDATED` | docker-compose, productor Python |
| Spark ventana 1 min | 25% | `LOCAL_VALIDATED` | Spark job con promedios |
| README/evidencias | 15% | `LOCAL_VALIDATED` | README + docs |
| Calidad/seguridad | 10% | `LOCAL_VALIDATED` | ConfigMaps, sin credenciales |

### PE4 — Flink Stateful (100% peso)

| Criterio | Peso | Estado | Evidencia |
|---|---|---|---|
| Flink stateful + watermarks + ventanas | 35% | `LOCAL_VALIDATED` | Java compilable, 7 tests JUnit ✅ |
| Checkpoints + tolerancia fallos | 30% | `LOCAL_VALIDATED` | Config 60s, recovery diseñado |
| IaC Flink | 15% | `LOCAL_VALIDATED` | `aws_kinesisanalyticsv2_application`, CI ✅ |
| README | 10% | `LOCAL_VALIDATED` | PE4_README.md completo |
| Evidencias | 10% | `BLOQUEADO_POR_COSTO` | Sin ejecución AWS |

### PE5 — Iceberg/Glue/Athena (100% peso)

| Criterio | Peso | Estado | Evidencia |
|---|---|---|---|
| Terraform/Glue/S3 | 30% | `LOCAL_VALIDATED` | Módulo lakehouse, CI ✅ |
| Flink → Iceberg sink | 32% | `LOCAL_VALIDATED` | IcebergSinkBuilder.java compilable |
| Consistencia transaccional | 18% | `IMPLEMENTED_UNVERIFIED` | Documentado, no probado en AWS |
| Verificación Athena | 12% | `IMPLEMENTED_UNVERIFIED` | SQL estático, sin ejecución real |
| Documentación | 8% | `LOCAL_VALIDATED` | PE5_README.md ✅ |

### PE6 — Redshift (100% peso)

| Criterio | Peso | Estado | Evidencia |
|---|---|---|---|
| Streaming schema + MV ≥3 campos | 35% | `IMPLEMENTED_UNVERIFIED` | SQL + 6 campos, CI ✅ |
| External schema Glue + JOIN | 25% | `IMPLEMENTED_UNVERIFIED` | SQL con JOIN streaming × Iceberg |
| Refresh + métricas | 15% | `IMPLEMENTED_UNVERIFIED` | AUTO REFRESH + queries monitoreo |
| IAM mínimo privilegio | 15% | `LOCAL_VALIDATED` | 4 policies, CI ✅ |
| SQL/docs/evidencia | 10% | `LOCAL_VALIDATED` | 5 archivos SQL + README |

### DAAT — Resumen global

| Criterio | Peso | Estado |
|---|---|---|
| Repositorio completo | 20% | `LOCAL_VALIDATED` — /terraform, /flink-app, /sql, /docs, /k8s |
| End-to-end verificable | 30% | `BLOQUEADO_POR_COSTO` — diseñado pero no ejecutado |
| Documento/diagramas | 20% | `LOCAL_VALIDATED` — este documento |
| Calidad IaC | 15% | `LOCAL_VALIDATED` — CI fmt+validate ✅ |
| Fallos/trade-offs | 15% | `IMPLEMENTED_UNVERIFIED` — análisis teórico documentado |

---

## 14. Riesgos, deuda técnica y decisiones ADR

### 14.1 Riesgos activos

| Riesgo | Severidad | Mitigación |
|---|---|---|
| Sin despliegue AWS real | Crítica | Documentación exhaustiva, consulta al docente |
| Consistencia entre rutas no probada | Alta | Reconciliación SQL diseñada, pendiente validación |
| Costos reales de Redshift Serverless | Alta | Solo 8 RPU base, sin auto-scaling |
| Exactamente-once no garantizado E2E | Media | Deduplicación por event_id, atomic commits Iceberg |
| Schema evolution no implementada | Baja | Contrato JSON estable, versionable con Schema Registry |

### 14.2 Decisiones arquitectónicas (ADR)

| ADR | Decisión | Justificación |
|---|---|---|
| ADR-001 | DynamoDB para state locking (no solo S3 native) | Rúbrica exige DynamoDB; provider moderno soporta S3 native locking |
| ADR-002 | S3 Gateway Endpoint en lugar de NAT | Ahorro de ~$35/mes sin impacto funcional para S3 |
| ADR-003 | Kinesis PROVISIONED vs ON-DEMAND | Costos predecibles y control explícito de shards |
| ADR-004 | Flink Java (no Python/SQL puro) | Mayor control sobre state, watermarks y procesamiento |
| ADR-005 | Iceberg v2 con Glue Catalog | Integración nativa AWS, time travel, schema evolution |
| ADR-006 | PE3 como ejercicio aislado | Evita contaminar la arquitectura AWS con infra no requerida |

### 14.3 Deuda técnica

- Monitoring/alerting no configurado (CloudWatch Alarms, SNS)
- Compactación de archivos Iceberg no automatizada
- Sin dead-letter queue para eventos que fallen en Flink
- Sin tests de integración (solo unitarios)
- Sin Enhanced Fan-Out (3 consumidores comparten throughput de lectura)

---

## 15. Conclusiones

Esta plataforma de streaming de datos en tiempo real demuestra un diseño completo y coherente que cubre las seis preentregas del capstone:

1. **Infraestructura reproducible** con Terraform modular, backend remoto y CI/CD
2. **Ingesta en tiempo real** con Kinesis Data Streams y Firehose
3. **Ejercicio distribuido** con Kafka, Kubernetes y Spark (PE3)
4. **Procesamiento stateful** con Apache Flink, Event Time, Watermarks y checkpoints
5. **Lakehouse** con Apache Iceberg, Glue Data Catalog y consultas Athena
6. **Analítica streaming** con Redshift Streaming Ingestion y JOINs con datos Iceberg

**Todo el código ha sido validado estáticamente** (CI/CD con `terraform fmt`, `validate`, `init`) y **localmente** (compilación Maven, tests JUnit). **Ningún recurso AWS fue provisionado** debido a la restricción de costo $0. Los criterios de rúbrica que requieren despliegue real están debidamente marcados como `BLOQUEADO_POR_COSTO`.

El repositorio contiene código desplegable desde cero, documentación completa por preentrega, y un runbook de despliegue para validación futura cuando exista presupuesto o créditos académicos disponibles.

---

## Anexos

### A. Estructura del repositorio

```
aws-streaming-data-platform/
├── .github/workflows/
│   └── terraform-validate.yml
├── terraform/
│   ├── bootstrap/
│   ├── modules/
│   │   ├── networking/
│   │   ├── identity/
│   │   ├── ingestion/
│   │   ├── analytics/
│   │   ├── lakehouse/
│   │   └── redshift/
│   └── environments/dev/
├── flink-app/
│   ├── pom.xml
│   └── src/
├── producer/
│   └── kinesis_producer.py
├── sql/
│   ├── athena_queries.sql
│   ├── 01_streaming_schema.sql
│   ├── 02_materialized_view.sql
│   ├── 03_external_schema_glue.sql
│   ├── 04_queries_and_joins.sql
│   └── 05_refresh_and_monitoring.sql
├── k8s/                          # PE3 (ejercicio aislado)
├── docs/
│   ├── PE1_README.md
│   ├── PE2_README.md
│   ├── PE3_README.md
│   ├── PE4_README.md
│   ├── PE5_README.md
│   ├── PE6_README.md
│   └── DAAT_Alvarez_Rodriguez_Capstone_RealTime.md
└── README.md
```

### B. Comandos de validación

```bash
# Terraform
cd terraform/environments/dev
terraform fmt -check -recursive -diff terraform/
terraform init -backend=false
terraform validate

# Flink (compilación + tests)
cd flink-app
mvn clean package          # compilar + tests
mvn test                   # solo tests

# Productor (validación local)
python -c "import producer.kinesis_producer"
```

### C. Referencias oficiales

- [AWS Kinesis Data Streams](https://docs.aws.amazon.com/streams/latest/dev/)
- [AWS Data Firehose](https://docs.aws.amazon.com/firehose/latest/dev/)
- [Apache Flink 1.19](https://nightlies.apache.org/flink/flink-docs-release-1.19/)
- [Apache Iceberg](https://iceberg.apache.org/docs/latest/)
- [AWS Glue Data Catalog](https://docs.aws.amazon.com/glue/latest/dg/)
- [Amazon Redshift Streaming Ingestion](https://docs.aws.amazon.com/redshift/latest/dg/materialized-view-streaming-ingestion.html)
- [Terraform AWS Provider](https://registry.terraform.io/providers/hashicorp/aws/latest/docs)
