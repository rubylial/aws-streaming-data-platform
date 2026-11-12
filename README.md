# AWS Real-Time Streaming Data Platform

Plataforma de streaming en tiempo real sobre servicios administrados de AWS, diseñada como proyecto Capstone de ingeniería de datos.

## Arquitectura

```
Producer JSON ──► Kinesis Data Streams ──┬──► Data Firehose ──► S3 (Bronze/Raw)
                                         │
                                         ├──► Managed Apache Flink ──► Apache Iceberg (S3 + Glue Catalog)
                                         │                                    │
                                         │                              Athena / Redshift Spectrum
                                         │
                                         └──► Redshift Streaming Ingestion ──► Materialized View
                                                                                    │
                                                                              JOIN con Iceberg
```

### Rutas de consumo

| Ruta | Origen | Destino | Propósito |
|---|---|---|---|
| **Bronze** | Kinesis → Firehose | S3 raw | Landing histórico y auditoría |
| **Lakehouse** | Kinesis → Flink | Iceberg (S3 + Glue Catalog) → Athena | Procesamiento stateful, tablas analíticas |
| **Analítica in-stream** | Kinesis → Redshift Streaming Ingestion | Materialized View → JOIN con Iceberg | Analítica SQL en tiempo real |

### Contrato de eventos

```json
{
  "event_id": "evt-20261103-001",
  "event_time": "2026-11-03T20:10:00Z",
  "merchant_id": "m-001",
  "amount": 128.50,
  "currency": "COP",
  "status": "APPROVED"
}
```

## Estructura del repositorio

```
├── terraform/
│   ├── bootstrap/          # Backend S3 + DynamoDB (state remoto)
│   ├── modules/
│   │   ├── networking/     # VPC, subredes, S3 Gateway Endpoint
│   │   ├── identity/       # IAM roles y policies
│   │   ├── ingestion/      # Kinesis Data Streams, Firehose, KMS
│   │   ├── processing/     # Managed Apache Flink
│   │   ├── lakehouse/      # Glue Data Catalog, S3 Iceberg
│   │   └── analytics/      # Redshift Serverless
│   └── environments/
│       └── dev/            # Root module del ambiente de desarrollo
├── flink-app/              # Aplicación Flink (Java)
├── scripts/producer/       # Productor JSON para Kinesis
├── sql/
│   ├── redshift/           # DDL y queries Redshift
│   └── athena/             # Queries Athena
├── k8s/                    # PE3: Kafka/Spark en Kubernetes (aislado)
├── docs/
│   ├── PROJECT_STATUS.md   # Estado actualizado del proyecto
│   ├── PLAN_OUTPUT.md      # Salida de terraform plan
│   ├── architecture/       # Diagramas
│   ├── adr/                # Registros de decisión arquitectónica
│   └── evidence/           # Evidencias por preentrega
└── .github/workflows/      # CI/CD (solo validación estática)
```

## Preentregas

   | PE | Alcance | Estado | README |
   |---|---|---|---|
   | PE1 | Terraform base, VPC, IAM, backend remoto | `LOCAL_VALIDATED` | — |
   | PE2 | Kinesis Data Streams + Firehose → S3 | `LOCAL_VALIDATED` | — |
   | PE3 | Kafka + Kubernetes + Spark (ejercicio aislado) | `LOCAL_VALIDATED` | [PE3_README](docs/PE3_README.md) |
   | PE4 | Managed Apache Flink stateful | `LOCAL_VALIDATED` | [PE4_README](docs/PE4_README.md) |
| PE5 | Lakehouse Iceberg + Glue + Athena | `IMPLEMENTED_UNVERIFIED` | [PE5_README](docs/PE5_README.md) |
   | PE6 | Redshift Streaming Ingestion + MV | `NOT_STARTED` | — |
   | DAAT | Documento de Arquitectura y Auditoría Técnica | `NOT_STARTED` | — |

## Validación local (costo cero)

Este proyecto se desarrolla bajo política de **costo cero** ($0 USD). Las validaciones se realizan localmente:

```bash
# Formato y sintaxis Terraform
terraform fmt -check -recursive
terraform validate

# Linting y seguridad
tflint --recursive
tfsec terraform/

# Tests unitarios del productor (Python)
python -m pytest scripts/producer/tests/ -v

# Flink local (mini-cluster)
cd flink-app && mvn package && flink run target/*.jar
```

Los despliegues AWS reales requieren aprobación explícita y se documentan como evidencia separada.

## Decisiones arquitectónicas

Ver [`docs/adr/`](docs/adr/) para los ADR del proyecto.

## Seguridad

- IAM con mínimo privilegio y condición de `iam:PassRole`
- KMS para cifrado de Kinesis y S3
- VPC privada con S3 Gateway Endpoint
- Backend Terraform cifrado (SSE-S3) con versionado y locking
- Sin credenciales en el repositorio — usar `terraform.tfvars.example`
- CI/CD limitado a validación estática (nunca `apply` automático)
