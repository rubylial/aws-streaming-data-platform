# PROJECT STATUS — AWS Real-Time Data Engineering Capstone

**Última actualización:** 2026-11-13
**Repositorio:** github.com/rubylial/aws-streaming-data-platform
**Branch principal:** main

## Estado general

| Entrega | Estado | CI | Evidencia AWS | Branch |
|---|---|---|---|---|
| PE1 — Terraform/VPC/IAM/Backend | `LOCAL_VALIDATED` | ✅ | ❌ (VPC no desplegada) | `main` |
| PE2 — Kinesis/Firehose/Productor | `AWS_VALIDATED` | ✅ | ✅ | `feature/ingesta-kinesis` → `main` |
| PE3 — Kafka/Kubernetes/Spark | `LOCAL_VALIDATED` | ✅ | N/A (local) | `feature/pe3-kafka-spark` → `main` |
| PE4 — Flink Stateful | `LOCAL_VALIDATED` | ✅ | ✅ (MiniCluster local) | `feature/pe4-flink` → `main` |
| PE5 — Iceberg/Glue/Athena | `AWS_VALIDATED` | ✅ | ✅ | `feature/pe5-iceberg` → `main` |
| PE6 — Redshift Streaming | `AWS_VALIDATED` | ✅ | ✅ | `feature/pe6-redshift` → `main` |
| DAAT — Documento final | `LOCAL_VALIDATED` | — | Parcial (E2E incompleto) | `main` |

## Restricción de costo

**Presupuesto: $0 USD real.** Validación AWS realizada exclusivamente en AWS Academy Learner Lab con créditos académicos precargados.

### Sesiones de validación AWS

**Sesión 1 — PE5 (2026-11-12, ~$0.05):**
- Glue database + 2 tablas Iceberg + Athena (SHOW TABLES, DESCRIBE, COUNT)

**Sesión 2 — PE6 + PE2 (2026-11-12, ~$3-4):**
- Redshift Provisioned ra3.large (Serverless bloqueado en Academy)
- Kinesis Data Stream (2 shards) + productor JSON (50 eventos)
- Firehose → S3 (buffering 5MB/60s)
- 13 queries SQL en Redshift Query Editor v2

### Validaciones locales sin costo
- `terraform fmt -check`, `terraform init -backend=false`, `terraform validate` (CI)
- PE4: `mvn clean package` → BUILD SUCCESS + MiniCluster test (9 tests)
- PE3: Docker/Kind local (Kafka + Spark Structured Streaming)
- Revisión estática de SQL (Redshift, Athena)

## PE2 — Evidencias AWS (2026-11-12)

| Criterio | Peso | Evidencia |
|---|---|---|
| Kinesis PROVISIONED 2 shards + productor partition keys variables | 30% | ✅ Stream detail + productor 50 eventos |
| Firehose → S3 (5MB/60s) | 25% | ✅ Firehose Monitoring + objetos S3 |
| IAM/KMS | 20% | ✅ Terraform repo (mínimo privilegio) |
| CloudWatch + pruebas | 15% | ✅ IncomingRecords metrics |
| Git branch | 10% | ✅ |

## PE4 — Correcciones del docente aplicadas

La devolución (53%) pidió 3 cosas, todas resueltas:

| Corrección pedida | Estado |
|---|---|
| Corregir error de tipos (shard assigner) + dependencias de build | ✅ `mvn package` BUILD SUCCESS |
| Instanciar módulo analytics en environments/dev | ✅ Instanciado en `main.tf` |
| Cambiar `code_content_type` a JAR | ✅ Aplicado |
| Ejecutar el job al menos en local + evidencia | ✅ MiniCluster test (logs en `docs/evidence/pe4/`) |

**Pendiente:** re-evaluación del docente.

## PE6 — Evidencias AWS (2026-11-12)

| Criterio | Peso | Evidencia |
|---|---|---|
| FROM KINESIS + MV ≥3 campos JSON | 35% | ✅ MV 6 campos, COUNT=50, procedencia Kinesis |
| FROM DATA CATALOG + JOIN | 25% | ✅ External schema Glue + JOIN streaming×Iceberg |
| Refresh/AUTO REFRESH + latencia | 15% | ✅ autorefresh=t + métricas latencia/freshness |
| IAM mínimo privilegio | 15% | ✅ Terraform repo |
| SQL + PDF + evidencia | 10% | ✅ 13 queries + PDF |

> **Nota:** Redshift Serverless bloqueado en Academy (`CreateNamespace` denegado).
> Se usó Redshift Provisioned ra3.large (único node type que soporta Streaming
> Ingestion además de Serverless). Tablas Iceberg vacías (Flink no ejecutado en
> AWS); el JOIN demuestra que ambos external schemas funcionan y se pueden cruzar.

## Criterios PENDIENTES

- **PE1**: VPC/subnets desplegadas → `BLOQUEADO_POR_COSTO` (declarativo; validable casi gratis en lab futuro sin NAT)
- **DAAT-02 E2E (30%)**: tramo Flink→Iceberg no ejecutado en AWS. Trazabilidad parcial
  probada (productor→Kinesis→Redshift MV vía sequence_number). Brecha documentada.

## Siguiente paso

1. Actualizar DAAT con evidencia AWS de hoy (secciones Kinesis/Firehose, Redshift, E2E)
2. Anonimizar Account ID en PDFs antes de entrega
3. (Opcional) Validar PE1 VPC en lab futuro (casi $0)
