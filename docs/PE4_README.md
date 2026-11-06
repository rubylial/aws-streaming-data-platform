# PE4 — Flink Stateful Streaming

## Arquitectura

```
Kinesis Data Streams
        │
        ▼
  Managed Apache Flink (Flink 1.19)
  ┌──────────────────────────────────────────┐
  │ Source: Kinesis Connector (FLIP-27)       │
  │ Deserialización: JSON → POJO             │
  │ Event Time + Watermarks (5s OOO)         │
  │                                          │
  │ ┌──────────────────────────────────────┐ │
  │ │ Ventana Tumbling 1 min               │ │
  │ │ keyBy(merchantId)                    │ │
  │ │ → count, sum, avg por merchant       │ │
  │ └──────────────────────────────────────┘ │
  │                                          │
  │ ┌──────────────────────────────────────┐ │
  │ │ Ventana Sliding 5 min / 1 min        │ │
  │ │ keyBy(merchantId)                    │ │
  │ │ → tendencia por merchant             │ │
  │ └──────────────────────────────────────┘ │
  │                                          │
  │ ┌──────────────────────────────────────┐ │
  │ │ Anomaly Detector (KeyedState)        │ │
  │ │ keyBy(merchantId)                    │ │
  │ │ ValueState: running sum + count      │ │
  │ │ → alerta si monto > 2x promedio     │ │
  │ └──────────────────────────────────────┘ │
  │                                          │
  │ Checkpoints: 60s, exactly-once           │
  │ State backend: HashMapStateBackend       │
  └──────────────────────────────────────────┘
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
    TransactionAnomalyDetector.java                 # KeyedState — detección anomalías
  src/test/java/com/streaming/flink/
    TransactionEventTest.java                       # Tests unitarios POJO/deserializer
    StreamingJobLocalTest.java                      # Test MiniCluster end-to-end
terraform/modules/analytics/
  main.tf                                           # Managed Flink + IAM + S3
  variables.tf                                      # Variables del módulo
  outputs.tf                                        # Outputs del módulo
docs/evidence/pe4/
  minicluster-test-output.txt                       # Salida completa del test MiniCluster
  build-success.txt                                 # Salida de compilación exitosa
```

## Compilación local

```bash
cd flink-app
mvn clean package -DskipTests
# Genera: target/flink-streaming-job-1.0.0.jar
```

## Ejecución local con MiniCluster

```bash
cd flink-app
mvn test -Dtest=StreamingJobLocalTest
```

El test ejecuta el pipeline completo en un Flink MiniCluster local:
- 14 eventos sintéticos en 3 merchants y 3 minutos de Event Time
- Incluye un evento tardío (late event) para validar watermarks
- Checkpoints habilitados cada 5 segundos
- Paralelismo 2
- Ventana Tumbling + Sliding + detección de anomalías con KeyedState

## Tests unitarios

```bash
cd flink-app
mvn test
```

Tests incluidos:
- `TransactionEventTest` (7 tests): deserialización JSON, event time, null handling, campos desconocidos, malformados
- `StreamingJobLocalTest` (1 test): pipeline completo en MiniCluster con checkpoints y KeyedState

## Componentes Flink

### Event Time y Watermarks

- **Estrategia**: `forBoundedOutOfOrderness(5 seconds)`
- **Timestamp Assigner**: extrae `event_time` del JSON, parsea ISO 8601 → epoch millis
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

### KeyedState — Detección de anomalías (`TransactionAnomalyDetector`)

- `RichFlatMapFunction` con **dos `ValueState`** explícitos:
  - `ValueState<Double> runningSum`: suma acumulada de montos por merchant
  - `ValueState<Long> runningCount`: conteo acumulado por merchant
- Inicializados en `open()` con `ValueStateDescriptor`
- Lógica: si `amount > 2.0 × (runningSum / runningCount)` → emite alerta
- Requiere mínimo 3 eventos para evaluar (evita falsos positivos al inicio)
- Estado incluido en checkpoints automáticamente
- Se recupera en caso de fallo sin pérdida de historial

### Checkpoints

| Parámetro | Valor (prod) | Valor (test) |
|---|---|---|
| Intervalo | 60,000 ms | 5,000 ms |
| Pausa mínima | 30,000 ms | 2,000 ms |
| Timeout | 120,000 ms | 30,000 ms |
| Máx. concurrentes | 1 | 1 |
| Fallos tolerados | 3 | — |

**Nota**: en Managed Flink, los checkpoints son administrados por el servicio.
Esto provee tolerancia a fallos y recuperación de estado. No equivale
automáticamente a exactly-once hacia todos los sinks — depende del sink.

## Cálculo de KPUs (Kinesis Processing Units)

La configuración de Managed Flink en Terraform define:

```hcl
parallelism_configuration {
  parallelism          = 2
  parallelism_per_kpu  = 1
  auto_scaling_enabled = false
}
```

**Cálculo**: `KPUs = parallelism / parallelism_per_kpu = 2 / 1 = 2 KPUs`

| Parámetro | Valor | Justificación |
|---|---|---|
| parallelism | 2 | Alineado con 2 shards Kinesis (1 subtask por shard) |
| parallelism_per_kpu | 1 | Cada KPU aporta 1 slot de paralelismo |
| auto_scaling | false | Control de costos; escalar requiere aprobación |
| **KPUs totales** | **2** | Mínimo viable para el stream de 2 shards |

**Recursos por KPU** (referencia AWS):
- 1 vCPU
- 4 GB de memoria
- 50 GB de almacenamiento administrado

**Costo estimado** (referencia, no ejecutado):
- 2 KPUs × $0.11/hr = $0.22/hr ≈ $158.40/mes (us-east-1)
- `BLOQUEADO_POR_COSTO`: no se despliega en la cuenta académica

## Terraform — Recursos declarados

| Recurso | Tipo | Propósito |
|---|---|---|
| S3 Bucket artefactos | `aws_s3_bucket` | JAR del job Flink |
| S3 Versioning | `aws_s3_bucket_versioning` | Historial de artefactos |
| S3 Public Access Block | `aws_s3_bucket_public_access_block` | Seguridad |
| CloudWatch Log Group | `aws_cloudwatch_log_group` | Logs de la aplicación |
| CloudWatch Log Stream | `aws_cloudwatch_log_stream` | Stream de logs |
| IAM Policy Kinesis | `aws_iam_role_policy` | Lectura del stream |
| IAM Policy S3 | `aws_iam_role_policy` | Artefactos y checkpoints |
| IAM Policy CloudWatch | `aws_iam_role_policy` | Escritura de logs |
| Managed Flink App | `aws_kinesisanalyticsv2_application` | Aplicación principal |

### Configuración de la aplicación Flink

- Runtime: `FLINK-1_19`
- Código: `code_content_type = "JAR"` → `flink-streaming-job-1.0.0.jar`
- Paralelismo: 2 (2 KPUs)
- Auto-scaling: deshabilitado (control de costos)
- Checkpoints: habilitados, 60s intervalo, CUSTOM
- Logging: INFO nivel aplicación
- Módulo cableado en `terraform/environments/dev/main.tf`

### Correcciones aplicadas (reentrega)

| Corrección | Archivo | Detalle |
|---|---|---|
| Error de tipos `KinesisShardAssigner` | `StreamingJob.java` | Lambda `(shard, numShards)` incompatible con FLIP-27; se usa `UniformShardAssigner` default |
| Dependencia faltante | `pom.xml` | Agregado `flink-table-api-java` y `flink-table-api-java-bridge` (requeridos por `IcebergSinkBuilder`) |
| Content type incorrecto | `analytics/main.tf` | Cambiado `"ZIPFILE"` → `"JAR"` |
| Módulo no instanciado | `dev/main.tf` | Agregado bloque `module "analytics"` con variables de ingestion e identity |

## Evidencias de ejecución local

Todas las evidencias se encuentran en `docs/evidence/pe4/`:

| Archivo | Contenido | Comando que lo generó |
|---|---|---|
| `minicluster-test-output.txt` | Salida completa del test MiniCluster con resultados de ventanas y anomalías | `mvn test -Dtest=StreamingJobLocalTest 2>&1 \| tee docs/evidence/pe4/minicluster-test-output.txt` |
| `build-success.txt` | Compilación exitosa del JAR (fat JAR con shade plugin) | `mvn clean package -DskipTests 2>&1 \| tee docs/evidence/pe4/build-success.txt` |

### Resultados observados (MiniCluster local)

**Ventana Tumbling (1 min)** — 7 agregaciones generadas:

```
[TUMBLING] MerchantAgg{merchant=merchant-001, window=[10:00:00Z -> 10:01:00Z], count=4, total=485.50, avg=121.38}
[TUMBLING] MerchantAgg{merchant=merchant-002, window=[10:00:00Z -> 10:01:00Z], count=2, total=620.00, avg=310.00}
[TUMBLING] MerchantAgg{merchant=merchant-003, window=[10:01:00Z -> 10:02:00Z], count=2, total=1700.00, avg=850.00}
[TUMBLING] MerchantAgg{merchant=merchant-001, window=[10:01:00Z -> 10:02:00Z], count=2, total=350.00, avg=175.00}
[TUMBLING] MerchantAgg{merchant=merchant-002, window=[10:01:00Z -> 10:02:00Z], count=1, total=800.00, avg=800.00}
[TUMBLING] MerchantAgg{merchant=merchant-001, window=[10:02:00Z -> 10:03:00Z], count=1, total=175.00, avg=175.00}
[TUMBLING] MerchantAgg{merchant=merchant-002, window=[10:02:00Z -> 10:03:00Z], count=1, total=90.00, avg=90.00}
```

**Ventana Sliding (5 min / 1 min)** — 20+ agregaciones deslizantes generadas. Ejemplo:

```
[SLIDING]  MerchantAgg{merchant=merchant-001, window=[09:56:00Z -> 10:01:00Z], count=4, total=485.50, avg=121.38}
[SLIDING]  MerchantAgg{merchant=merchant-001, window=[09:57:00Z -> 10:02:00Z], count=6, total=835.50, avg=139.25}
[SLIDING]  MerchantAgg{merchant=merchant-001, window=[09:58:00Z -> 10:03:00Z], count=7, total=1010.50, avg=144.36}
```

**Detección de anomalías (KeyedState)** — 1 alerta generada:

```
[ANOMALY]  ANOMALY DETECTED | merchant=merchant-001 | event_id=evt-006 | amount=300.00 | avg=141.83 | threshold=283.67 | ratio=2.12x
```

- El evento `evt-006` ($300.00) superó el umbral de 2x el promedio histórico ($141.83) del merchant-001
- El detector necesitó 3 eventos previos antes de evaluar (MIN_EVENTS_FOR_DETECTION = 3)

**Estado del test**: `Tests run: 1, Failures: 0, Errors: 0, Skipped: 0` — **BUILD SUCCESS**

### Código fuente del test y detector

| Archivo | Clase | Propósito |
|---|---|---|
| `src/test/java/.../StreamingJobLocalTest.java` | `StreamingJobLocalTest` | Test MiniCluster con 14 eventos sintéticos, 3 merchants, 3 minutos de Event Time, late event, checkpoints 5s |
| `src/main/java/.../TransactionAnomalyDetector.java` | `TransactionAnomalyDetector` | `RichFlatMapFunction` con `ValueState<Double>` + `ValueState<Long>` para detección de anomalías por merchant |

## Limitaciones académicas

| Aspecto | Estado | Nota |
|---|---|---|
| Compilación del JAR | ✅ `LOCAL_VALIDATED` | `mvn clean package` exitoso |
| Tests unitarios (POJO/deserializer) | ✅ `LOCAL_VALIDATED` | 7 tests pasan |
| Test MiniCluster (pipeline completo) | ✅ `LOCAL_VALIDATED` | Tumbling + Sliding + KeyedState anomalías verificados |
| KeyedState (ValueState) | ✅ `LOCAL_VALIDATED` | `TransactionAnomalyDetector` con 2 ValueState — anomalía detectada correctamente |
| Terraform validate | ⏳ `PENDIENTE` | Requiere providers instalados |
| Despliegue Managed Flink | ❌ `BLOQUEADO_POR_COSTO` | 2 KPUs ≈ $0.22/hr |
| Checkpoints en ejecución real | ❌ `BLOQUEADO_POR_COSTO` | Solo verificable en Managed Flink |
| Recuperación de fallos real | ❌ `BLOQUEADO_POR_COSTO` | Solo verificable en Managed Flink |
| Latencia medida | ❌ `BLOQUEADO_POR_COSTO` | Solo verificable en Managed Flink |

## Criterios de rúbrica PE4

| Criterio | Peso | Estado | Evidencia |
|---|---|---|---|
| Consumo Kinesis + Event Time + Watermarks + Ventanas + State | 35% | `LOCAL_VALIDATED` | Código + test MiniCluster + KeyedState con evidencia |
| Checkpoints, fallos, recuperación | 30% | `IMPLEMENTED_UNVERIFIED` | Config en código + Terraform (verificación real requiere Managed Flink) |
| `aws_kinesisanalyticsv2_application` + IAM | 15% | `IMPLEMENTED_UNVERIFIED` | HCL válido + módulo cableado |
| README y documentación | 10% | ✅ `LOCAL_VALIDATED` | Este documento con evidencias |
| Evidencias reales | 10% | `BLOQUEADO_POR_COSTO` | Solo con despliegue AWS |
