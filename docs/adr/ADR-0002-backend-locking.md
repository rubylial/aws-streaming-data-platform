# ADR-0002 — Backend S3 y mecanismo de locking Terraform

**Estado:** Proposed
**Fecha:** 2026-11-03
**Autor:** Ruby Alvarez

## Contexto

La rúbrica de PE1 (18%) solicita estado remoto en S3 cifrado con locking. El mecanismo tradicional usa una tabla DynamoDB para el lock.

Sin embargo, a partir de Terraform ~1.10, HashiCorp introdujo `use_lockfile = true` como alternativa nativa en el backend S3 (locking mediante un archivo `.tflock` en el mismo bucket), marcando DynamoDB locking como **deprecated**.

Referencia: https://developer.hashicorp.com/terraform/language/backend/s3

## Decisión

Para la entrega académica PE1:

1. **Implementar DynamoDB locking** en el bootstrap (`terraform/bootstrap/main.tf`) porque la rúbrica lo exige explícitamente.
2. **Documentar la deprecación** y la alternativa `use_lockfile` como nota en el código y en este ADR.
3. **No introducir `use_lockfile`** hasta verificar que la versión de Terraform del entorno lo soporte (>= 1.10).

## Configuración implementada

```hcl
# terraform/environments/dev/backend.tf
terraform {
  backend "s3" {
    bucket         = "streaming-data-platform-tf-state-dev"
    key            = "environments/dev/terraform.tfstate"
    region         = "us-east-1"
    encrypt        = true
    dynamodb_table = "streaming-data-platform-tf-locks-dev"
  }
}
```

### Características de seguridad
- **Cifrado**: SSE-KMS (`aws:kms`) en el bucket S3
- **Versionado**: habilitado para recuperación de state corrupto
- **Bloqueo de acceso público**: las 4 opciones activadas
- **Locking**: tabla DynamoDB con `LockID` como hash key, `PAY_PER_REQUEST`
- **Separación**: el bootstrap usa state local; el state principal va al bucket

## Migración futura

Cuando se adopte Terraform >= 1.10 en el proyecto:

```hcl
terraform {
  backend "s3" {
    bucket       = "streaming-data-platform-tf-state-dev"
    key          = "environments/dev/terraform.tfstate"
    region       = "us-east-1"
    encrypt      = true
    use_lockfile = true
    # dynamodb_table ya no necesario
  }
}
```

Pasos:
1. Verificar versión: `terraform version`
2. Remover `dynamodb_table` del backend
3. Agregar `use_lockfile = true`
4. `terraform init -migrate-state`
5. Verificar que el lock funciona con operaciones concurrentes
6. (Opcional) eliminar tabla DynamoDB tras validación

## Riesgos

| Riesgo | Mitigación |
|---|---|
| Rúbrica rechaza si no hay DynamoDB | Implementamos DynamoDB — cumplimiento directo |
| DynamoDB genera costo | PAY_PER_REQUEST con cero uso = ~$0, pero no garantizado |
| `use_lockfile` no soportado en versión instalada | No lo usamos hasta verificar |
| State corrupto sin versionado | Versionado S3 habilitado |
