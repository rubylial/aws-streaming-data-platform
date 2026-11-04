# PE2 — Ingesta con Kinesis Data Streams y Data Firehose

## Arquitectura

Productor Python ──► Kinesis Data Streams (PROVISIONED, 2 shards)
│
├──► Data Firehose ──► S3 Bronze (raw/)
│ buffer: 5 MB / 60 s
│ prefix: year/month/day/hour
│
├──► [PE4] Managed Flink ──► Iceberg
│
└──► [PE6] Redshift Streaming Ingestion


## Contrato de evento

| Campo | Tipo | Descripción |
|---|---|---|
| `event_id` | string | UUID único para idempotencia/dedupe |
| `event_time` | string | ISO 8601 UTC (`2024-11-04T05:00:00Z`) |
| `merchant_id` | string | ID del comercio (PartitionKey) |
| `amount` | number | Monto de la transacción |
| `currency` | string | Moneda (`COP`) |
| `status` | string | `completed`, `pending`, `failed`, `refunded` |

**PartitionKey = `merchant_id`**: distribuye registros entre shards por comercio, asegurando orden por merchant dentro de cada shard.

## Estructura de archivos PE2

terraform/modules/ingestion/
main.tf # Kinesis, Firehose, S3, KMS, CloudWatch
variables.tf # Variables con validaciones
outputs.tf # Outputs del módulo
scripts/producer/
producer.py # Productor con modos --local y --stream
requirements.txt # Dependencias (boto3)
sample_events.json # Eventos sintéticos con duplicado intencional
tests/
test_producer.py # Tests unitarios con mocks de boto3


## Ejecución del productor

### Modo local (sin AWS, sin costo)

```bash
cd scripts/producer
python producer.py --local --count 10
```

Salida: eventos JSON en stdout. Útil para verificar el contrato y como evidencia.

### Modo stream (requiere AWS — BLOQUEADO_POR_COSTO)

```bash
# NO ejecutar sin aprobación
python producer.py --stream streaming-data-platform-events-dev --count 50
```

### Envío desde archivo

```bash
# NO ejecutar sin aprobación
python producer.py --file streaming-data-platform-events-dev sample_events.json
```

## Tests

```bash
# Desde la raíz del repo
python -m pytest tests/test_producer.py -v
```

Los tests NO requieren AWS ni boto3 real — usan mocks.

## Terraform — Recursos creados

| Recurso | Tipo | Propósito |
|---|---|---|
| KMS Key + Alias | `aws_kms_key` | CMK para cifrado Kinesis y S3 |
| Kinesis Stream | `aws_kinesis_stream` | Stream PROVISIONED, 2 shards |
| S3 Bronze Bucket | `aws_s3_bucket` | Destino raw de Firehose |
| Firehose | `aws_kinesis_firehose_delivery_stream` | Kinesis → S3 (5 MB / 60 s) |
| CloudWatch Log Group | `aws_cloudwatch_log_group` | Logs de entrega Firehose |
| CW Alarm Iterator Age | `aws_cloudwatch_metric_alarm` | Detecta lag de consumidores |
| CW Alarm Throttle | `aws_cloudwatch_metric_alarm` | Detecta throttling de escritura |

## Monitoreo CloudWatch

- **IteratorAge**: si supera 60 s, un consumidor está rezagado.
- **WriteProvisionedThroughputExceeded**: si > 0, el productor excede la capacidad de los shards.

## IAM (complemento al módulo identity)

El módulo ingestion agrega dos policies al rol Firehose existente:
- `kms-streaming-access`: `kms:Decrypt` y `kms:GenerateDataKey` sobre la CMK
- `cloudwatch-logs-delivery`: `logs:PutLogEvents` sobre el log group de Firehose

## Seguridad

- KMS CMK con rotación automática
- S3 bucket sin acceso público, con versionado y cifrado
- Sin credenciales en código — el productor usa el chain de credenciales de boto3
- Datos sintéticos únicamente

## Limitaciones académicas

| Aspecto | Estado |
|---|---|
| Kinesis stream desplegado | `BLOQUEADO_POR_COSTO` |
| Firehose entregando a S3 | `BLOQUEADO_POR_COSTO` |
| Objetos visibles en S3 | `BLOQUEADO_POR_COSTO` |
| CloudWatch metrics reales | `BLOQUEADO_POR_COSTO` |
| Productor modo local | ✅ Ejecutable sin AWS |
| Tests unitarios | ✅ Ejecutable sin AWS |
| Terraform validate | ✅ Verificado en CI |

## Criterios de rúbrica PE2

| Criterio | Peso | Estado |
|---|---|---|
| Kinesis PROVISIONED ≥2 shards + productor | 30% | `IMPLEMENTED_UNVERIFIED` |
| Firehose → S3 buffer 5MB/60s | 25% | `IMPLEMENTED_UNVERIFIED` |
| IAM / KMS | 20% | `IMPLEMENTED_UNVERIFIED` |
| Pruebas / CloudWatch / docs | 15% | `IMPLEMENTED_UNVERIFIED` |
| Git (rama feature/ingesta-kinesis) | 10% | `IMPLEMENTED_UNVERIFIED` |
