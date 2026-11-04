# ============================================================
# Backend S3 Remoto — Environment Dev
# ============================================================
# Requisito PE1 (18%): estado remoto cifrado con locking.
#
# PREREQUISITO: ejecutar terraform/bootstrap/ primero para
# crear el bucket S3 y la tabla DynamoDB.
#
# Valores deben coincidir con los outputs del bootstrap.
# Usar -backend-config en init para evitar hardcodear:
#   terraform init \
#     -backend-config="bucket=streaming-data-platform-tf-state-dev" \
#     -backend-config="dynamodb_table=streaming-data-platform-tf-locks-dev"
#
# BLOQUEADO_POR_COSTO: No ejecutar init contra AWS sin aprobación.
# ============================================================

terraform {
  backend "s3" {
    # Valores por defecto — sobreescribir con -backend-config
    bucket         = "streaming-data-platform-tf-state-dev"
    key            = "environments/dev/terraform.tfstate"
    region         = "us-east-1"
    encrypt        = true
    dynamodb_table = "streaming-data-platform-tf-locks-dev"
  }
}
