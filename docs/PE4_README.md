# PE4 — Flink Stateful Streaming

## Arquitectura

```
Kinesis Data Streams
        │
        ▼
  Managed Apache Flink (Flink 1.19)
  ┌──────────────────────────────────────┐
  │ Source: Kinesis Connector             │
  │ Deserialización: JSON → POJO         │
  │ Event Time + Watermarks (5s OOO)     │
  │                                      │
  │ ┌──────────────────────────────────┐ │
  │ │ Ventana Tumbling 1 min           │ │
  │ │ keyBy(merchantId)                │ │
  │ │ → count, sum, avg por merchant   │ │
  │ └──────────────────────────────────┘ │
  │                                      │
  │ ┌──────────────────────────────────┐ │
  │ │ Ventana Sliding 5 min / 1 min    │ │
  │ │ keyBy(merchantId)                │ │
  │ │ → tendencia por merchant         │ │
  │ └──────────────────────────────────┘ │
  │                                      │
  │ Checkpoints: 60s, exactly-once       │
  │ State backend: HashMapStateBackend   │
  └──────────────────────────────────────┘
        │
        ▼
  Print Sink (→ PE5: Iceberg sink)
```

## Contrato de evento (mismo que PE2)

| Campo | Tipo | Uso en Flink |
|---|---|---|
| `event_id` | string | Identificación / dedupe |
| `event_time` | string ISO 8601 | **Event Time** para watermarks |
| `merchant_id` | string | **Key** para particionado |
| `amount` | double | Agregación (sum, avg) |
| `currency` | string | Contexto |
| `status` | string | Filtrado futuro |

## Estructura de archivos

```
flink-app/
  pom.xml                                          # Maven con Flink 1.19
  src/main/java/com/streaming/flink/
    StreamingJob.java                               # Job principal
    TransactionEvent.java                           # POJO del evento
    TransactionDeserializer.java                    # JSON deserializer
    MerchantAggregation.java                        # Resultado de ventana
  src/main/resources/
    log4j2.properties                               # Logging config
  src/test/java/com/streaming/flink/
    TransactionEventTest.java                       # Tests unitarios
terraform/modules/analytics/
  main.tf                                           # Managed Flink + IAM + S3
  variables.tf                                      # Variables del módulo
  outputs.tf                                        # Outputs del módulo
```

## Compilación local

```bash
cd flink-app
mvn clean package -DskipTests
# Genera: target/flink-streaming-job-1.0.0.jar
```

## Tests

```bash
cd flink-app
mvn test
```

Tests incluidos (7 tests):
- Deserialización JSON válido
- Event time a epoch millis
- Campos parciales / null
- Campos desconocidos ignorados
- Eventos malformados → null
- Deserializer con evento válido
- MerchantAggregation toString

## Componentes Flink

### Event Time y Watermarks

- **Estrategia**: `forBoundedOutOfOrderness(5 seconds)`
- **Timestamp Assigner**: extrae `event_time` del JSON, parsea ISO 8601
- **Idleness**: 30 segundos (avanza watermark si una partición no recibe datos)

### Ventana Tumbling (1 minuto)

- `keyBy(merchantId)` → `TumblingEventTimeWindows.of(1 minute)`
- Agregación incremental con `AggregateFunction`: count + sum
- `ProcessWindowFunction` agrega metadata de ventana (start, end)
- Output: `MerchantAggregation{merchant, window, count, total, avg}`

### Ventana Sliding (5 min / slide 1 min)

- Misma agregación pero con ventana deslizante
- Cada merchant tiene un resultado cada minuto cubriendo los últimos 5
- Útil para detectar tendencias

### Checkpoints

- Intervalo: 60 segundos
- Pausa mínima entre checkpoints: 30 segundos
- Timeout: 120 segundos
- Máx. concurrentes: 1
- Tolerancia a fallos: 3 checkpoints fallidos

**Nota**: en Managed Flink, los checkpoints son administrados por el servicio.
Esto provee tolerancia a fallos y recuperación de estado. No equivale
automáticamente a exactly-once hacia todos los sinks — depende del sink.

## Terraform — Recursos declarados

| Recurso | Tipo | Propósito |
|---|---|---|
| S3 Bucket artefactos | `aws_s3_bucket` | JAR del job Flink |
| CloudWatch Log Group | `aws_cloudwatch_log_group` | Logs de la aplicación |
| IAM Policy Kinesis | `aws_iam_role_policy` | Lectura del stream |
| IAM Policy S3 | `aws_iam_role_policy` | Artefactos y checkpoints |
| IAM Policy CloudWatch | `aws_iam_role_policy` | Escritura de logs |
| Managed Flink App | `aws_kinesisanalyticsv2_application` | Aplicación principal |

### Configuración de la aplicación Flink

- Runtime: `FLINK-1_19`
- Paralelismo: 2
- Auto-scaling: deshabilitado (control de costos)
- Checkpoints: habilitados, 60s intervalo
- Logging: INFO nivel aplicación

## Limitaciones académicas

| Aspecto | Estado |
|---|---|
| Compilación del JAR | ✅ `mvn clean package` local |
| Tests unitarios | ✅ `mvn test` local |
| Terraform validate | ✅ CI pipeline |
| Despliegue Managed Flink | `BLOQUEADO_POR_COSTO` |
| Checkpoints en ejecución | `BLOQUEADO_POR_COSTO` |
| Recuperación de fallos real | `BLOQUEADO_POR_COSTO` |
| Latencia medida | `BLOQUEADO_POR_COSTO` |

## Criterios de rúbrica PE4

| Criterio | Peso | Estado |
|---|---|---|
| Consumo Kinesis + Event Time + Watermarks + Ventanas + State | 35% | `IMPLEMENTED_UNVERIFIED` |
| Checkpoints, fallos, recuperación | 30% | `IMPLEMENTED_UNVERIFIED` |
| `aws_kinesisanalyticsv2_application` + IAM | 15% | `IMPLEMENTED_UNVERIFIED` |
| README y documentación | 10% | `IMPLEMENTED_UNVERIFIED` |
| Evidencias reales | 10% | `BLOQUEADO_POR_COSTO` |
