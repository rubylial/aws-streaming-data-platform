# PROJECT STATUS — AWS Real-Time Streaming Data Platform

> Última actualización: 2026-11-03
> Branch de trabajo: `feature/pe1-terraform-base`
> Commit de referencia: pendiente (repo en estado initial commit)

## Resumen ejecutivo

El repositorio se encuentra en estado inicial (un solo commit con README vacío). Se está construyendo la infraestructura base de Terraform (PE1) como primer incremento.

## Estado por preentrega

### PE1 — Terraform, Networking y Seguridad

| Criterio | Peso | Estado | Evidencia | Archivos |
|---|---|---|---|---|
| Modularización y reutilización | 22% | IMPLEMENTED_UNVERIFIED | Módulos creados, pendiente validate | `terraform/modules/*`, `terraform/environments/dev/` |
| VPC privada multi-AZ + S3 GW Endpoint | 28% | IMPLEMENTED_UNVERIFIED | HCL presente, sin despliegue AWS | `terraform/modules/networking/` |
| IAM mínimo privilegio | 22% | IMPLEMENTED_UNVERIFIED | Roles y policies declarados | `terraform/modules/identity/` |
| Backend S3 cifrado + locking | 18% | IMPLEMENTED_UNVERIFIED | Bootstrap y backend config | `terraform/bootstrap/`, `terraform/environments/dev/backend.tf` |
| HCL, README, PLAN_OUTPUT | 10% | PROPOSED | README actualizado, PLAN_OUTPUT pendiente | `README.md`, `docs/PLAN_OUTPUT.md` |

**Estado global PE1:** IMPLEMENTED_UNVERIFIED
**Pendientes bloqueantes:**
- [ ] `terraform fmt -check` exitoso
- [ ] `terraform validate` exitoso (requiere provider mirror o init controlado)
- [ ] `terraform plan` capturado en PLAN_OUTPUT.md (requiere credenciales AWS — BLOQUEADO_POR_COSTO hasta aprobación)
- [ ] Despliegue AWS real (BLOQUEADO_POR_COSTO)

### PE2 — Kinesis + Firehose

| Criterio | Peso | Estado |
|---|---|---|
| Kinesis PROVISIONED ≥2 shards + productor | 30% | NOT_STARTED |
| Firehose → S3 (buffer 5MB/60s) | 25% | NOT_STARTED |
| IAM / KMS | 20% | NOT_STARTED |
| Pruebas / CloudWatch / docs | 15% | NOT_STARTED |
| Git branch feature/ingesta-kinesis | 10% | NOT_STARTED |

### PE3 — Kafka / Kubernetes / Spark (ejercicio aislado)

| Criterio | Peso | Estado |
|---|---|---|
| Kubernetes Deployments/Services YAML | 30% | NOT_STARTED |
| Kafka topic urban_sensors + productor | 20% | NOT_STARTED |
| Spark Structured Streaming (ventana 1 min) | 25% | NOT_STARTED |
| README y evidencias | 15% | NOT_STARTED |
| Calidad, seguridad, limpieza | 10% | NOT_STARTED |

### PE4 — Flink Stateful

| Criterio | Peso | Estado |
|---|---|---|
| Consumo Kinesis, Event Time, watermarks, ventanas | 35% | NOT_STARTED |
| Checkpoints, tolerancia a fallos | 30% | NOT_STARTED |
| Terraform Managed Flink + IAM | 15% | NOT_STARTED |
| README | 10% | NOT_STARTED |
| Evidencias | 10% | NOT_STARTED |

### PE5 — Lakehouse Iceberg/Glue

| Criterio | Peso | Estado |
|---|---|---|
| Terraform Glue/S3/IAM | 30% | NOT_STARTED |
| Sink Flink → Iceberg | 32% | NOT_STARTED |
| Consistencia transaccional | 18% | NOT_STARTED |
| Verificación Athena/schema | 12% | NOT_STARTED |
| Documentación | 8% | NOT_STARTED |

### PE6 — Redshift Streaming Ingestion

| Criterio | Peso | Estado |
|---|---|---|
| External schema FROM KINESIS + MV (≥3 campos) | 35% | NOT_STARTED |
| External schema FROM DATA CATALOG + JOIN | 25% | NOT_STARTED |
| Refresh / latencia | 15% | NOT_STARTED |
| IAM | 15% | NOT_STARTED |
| SQL / PDF / evidencia | 10% | NOT_STARTED |

### DAAT — Proyecto Final

| Criterio | Peso | Estado |
|---|---|---|
| Repositorio organizado | 20% | NOT_STARTED |
| End-to-end verificable | 30% | NOT_STARTED |
| Documento / diagramas | 20% | NOT_STARTED |
| Calidad IaC | 15% | NOT_STARTED |
| Fallos / backpressure / trade-offs | 15% | NOT_STARTED |

## Decisiones abiertas

| ID | Decisión | Estado | ADR |
|---|---|---|---|
| D-01 | Versión de Terraform y provider AWS | Pendiente | — |
| D-02 | DynamoDB locking vs S3 lockfile | Propuesto | ADR-0002 |
| D-03 | NAT Gateway vs VPC endpoints Interface | Pendiente | — |
| D-04 | Flink runtime y versión de conectores | Pendiente | — |
| D-05 | Catálogo Iceberg: Glue vs Hadoop local | Pendiente | — |

## Limitaciones académicas

Este proyecto se desarrolla bajo **política de costo cero**. Las siguientes actividades están `BLOQUEADO_POR_COSTO` hasta aprobación explícita:

- `terraform apply` contra cuenta AWS
- Creación de VPC, NAT Gateway, VPC endpoints
- Kinesis Data Streams, Firehose
- Managed Apache Flink
- Glue Data Catalog, Athena queries
- Redshift Serverless / cluster
- CloudWatch dashboards con métricas reales

Las validaciones se realizan localmente (`fmt`, `validate`, `tflint`, tests unitarios, Flink mini-cluster, Iceberg con catálogo Hadoop). Los resultados locales se etiquetan como `LOCAL_VALIDATED`, nunca como `AWS_DEPLOYED` o `E2E_VERIFIED`.
