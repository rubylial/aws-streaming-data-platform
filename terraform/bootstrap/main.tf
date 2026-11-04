## ============================================================
# Bootstrap — Backend Remoto (S3 + DynamoDB)
# ============================================================
# Crea los recursos necesarios para almacenar el estado de
# Terraform de forma remota y segura.
#
# Recursos:
# - S3 Bucket: almacena terraform.tfstate cifrado con SSE-KMS
# - DynamoDB Table: proporciona locking para evitar escrituras
#   concurrentes (requerido por rúbrica PE1 — ver ADR-0002)
#
# Ejecución:
#   cd terraform/bootstrap
#   terraform init
#   terraform plan -out=bootstrap.tfplan
#   terraform apply bootstrap.tfplan
#
# BLOQUEADO_POR_COSTO: No ejecutar sin aprobación explícita.
# Este módulo SÍ requiere AWS real para crear el backend.
# ============================================================

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "terraform-bootstrap"
    }
  }
}

# ============================================================
# Variables
# ============================================================

variable "project_name" {
  description = "Nombre del proyecto para naming de recursos"
  type        = string
  default     = "streaming-data-platform"
}

variable "environment" {
  description = "Ambiente (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "aws_region" {
  description = "Región AWS"
  type        = string
  default     = "us-east-1"
}

# ============================================================
# S3 Bucket — Estado de Terraform
# ============================================================
# - Versionado habilitado para historial de estados
# - Cifrado SSE-KMS (por defecto aws/s3 key)
# - Bloqueo de acceso público
# - prevent_destroy como salvaguarda
# ============================================================

resource "aws_s3_bucket" "terraform_state" {
  bucket = "${var.project_name}-tf-state-${var.environment}"

  tags = {
    Name    = "${var.project_name}-tf-state-${var.environment}"
    Purpose = "terraform-backend"
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_s3_bucket_versioning" "terraform_state" {
  bucket = aws_s3_bucket.terraform_state.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "terraform_state" {
  bucket = aws_s3_bucket.terraform_state.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "terraform_state" {
  bucket = aws_s3_bucket.terraform_state.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ============================================================
# DynamoDB Table — State Locking
# ============================================================
# Requerido por rúbrica PE1. Previene escrituras concurrentes
# al estado de Terraform.
#
# Nota: DynamoDB locking está deprecado en Terraform >= 1.10
# a favor de use_lockfile. Se mantiene por exigencia académica.
# Ver ADR-0002 para decisión y plan de migración.
# ============================================================

resource "aws_dynamodb_table" "terraform_locks" {
  name         = "${var.project_name}-tf-locks-${var.environment}"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "LockID"

  attribute {
    name = "LockID"
    type = "S"
  }

  tags = {
    Name    = "${var.project_name}-tf-locks-${var.environment}"
    Purpose = "terraform-state-locking"
  }
}

# ============================================================
# Outputs
# ============================================================

output "state_bucket_name" {
  description = "Nombre del bucket S3 para el estado de Terraform"
  value       = aws_s3_bucket.terraform_state.id
}

output "state_bucket_arn" {
  description = "ARN del bucket S3 para el estado de Terraform"
  value       = aws_s3_bucket.terraform_state.arn
}

output "locks_table_name" {
  description = "Nombre de la tabla DynamoDB para locking"
  value       = aws_dynamodb_table.terraform_locks.name
}

output "locks_table_arn" {
  description = "ARN de la tabla DynamoDB para locking"
  value       = aws_dynamodb_table.terraform_locks.arn
}

output "backend_config" {
  description = "Configuración de backend para copiar en environments/dev/backend.tf"
  value       = <<-EOT
    backend "s3" {
      bucket         = "${aws_s3_bucket.terraform_state.id}"
      key            = "environments/dev/terraform.tfstate"
      region         = "${var.aws_region}"
      encrypt        = true
      dynamodb_table = "${aws_dynamodb_table.terraform_locks.name}"
    }
  EOT
}
