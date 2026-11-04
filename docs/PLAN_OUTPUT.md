# PLAN_OUTPUT.md — Salida de `terraform plan`

> **Estado:** PENDIENTE — no se ha ejecutado `terraform plan` contra AWS.
>
> Este archivo se actualizará con la salida real de `terraform plan` cuando
> se autorice la ejecución contra una cuenta AWS con credenciales válidas.

## Comando esperado

```bash
cd terraform/environments/dev

# Inicializar (requiere backend S3 existente del bootstrap)
terraform init \
  -backend-config="bucket=streaming-data-platform-tf-state-dev" \
  -backend-config="dynamodb_table=streaming-data-platform-tf-locks-dev"

# Plan
terraform plan \
  -var-file=terraform.tfvars \
  -out=dev.tfplan \
  | tee plan-output.txt
```

## Recursos esperados en el plan

Basado en el código HCL de `terraform/environments/dev/` y los módulos `networking/` e `identity/`:

### Módulo `networking`
| # | Recurso | Tipo |
|---|---|---|
| 1 | `aws_vpc.main` | VPC |
| 2-3 | `aws_subnet.private[0-1]` | Subredes privadas (2 AZs) |
| 4 | `aws_route_table.private` | Route table |
| 5-6 | `aws_route_table_association.private[0-1]` | Asociaciones RT-subnet |
| 7 | `aws_vpc_endpoint.s3` | S3 Gateway Endpoint |
| 8 | `aws_security_group.internal` | SG base |

### Módulo `identity`
| # | Recurso | Tipo |
|---|---|---|
| 9 | `aws_iam_role.firehose_delivery` | Rol Firehose |
| 10 | `aws_iam_role_policy.firehose_s3_delivery` | Policy Firehose |
| 11 | `aws_iam_role.flink_execution` | Rol Flink |
| 12-15 | `aws_iam_role_policy.flink_*` | Policies Flink (4) |
| 16 | `aws_iam_role.glue_service` | Rol Glue |
| 17 | `aws_iam_role_policy_attachment.glue_service_base` | Policy base Glue |
| 18 | `aws_iam_role_policy.glue_s3_lakehouse` | Policy S3 Glue |
| 19 | `aws_iam_role.redshift_service` | Rol Redshift |
| 20-22 | `aws_iam_role_policy.redshift_*` | Policies Redshift (3) |
| 23 | `aws_iam_policy.passrole_control` | PassRole control |

**Total esperado: ~23 recursos a crear.**

## Salida real del plan

```
PENDIENTE — pegar aquí la salida de `terraform plan` cuando se ejecute.
Fecha de ejecución: ____
Región: ____
Cuenta (últimos 4 dígitos): ____
```

## Notas

- `terraform plan` es una operación de **lectura** que no crea recursos, pero
  requiere credenciales AWS válidas y puede generar llamadas API de lectura.
- Bajo política de costo cero, incluso `plan` se trata como prueba controlada.
- La salida se anonimiza antes de commitear (sin account IDs completos).
