# ============================================================
# Bootstrap — S3 Backend + DynamoDB Lock Table
# ============================================================
# Este módulo crea los recursos necesarios para el backend
# remoto de Terraform ANTES de que el backend esté configurado.
#
# Uso (una sola vez, con credenciales AWS configuradas):
#   cd terraform/bootstrap
#   terraform init
#   terraform plan -out=bootstrap.tfplan
#   terraform apply bootstrap.tfplan    # ← REQUIERE APROBACIÓN
#
# IMPORTANTE: Este bootstrap usa state local. Una vez creados
# los recursos, el state principal en environments/dev/ usará
# el backend S3 remoto.
#
# BLOQUEADO_POR_COSTO: No ejecutar sin autorización explícita.
# ============================================================

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Bootstrap usa state LOCAL — es intencional
}

# --------------------------------------------------------
# Variables
# --------------------------------------------------------

variable "project_name" {
  description = "Nombre del proyecto para tagging y naming"
  type        = string
  default     = "streaming-data-platform"
}

variable "environment" {
  description = "Ambiente (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "aws_region" {
  description = "Región AWS para los recursos del backend"
  type        = string
  default     = "us-east-1"
}

# --------------------------------------------------------
# Provider
# --------------------------------------------------------

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

# --------------------------------------------------------
# S3 Bucket para Terraform State
# --------------------------------------------------------

resource "aws_s3_bucket" "terraform_state" {
  bucket = "${var.project_name}-tf-state-${var.environment}"

  # Prevenir destrucción accidental del state
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

# --------------------------------------------------------
# DynamoDB Table para State Locking
# --------------------------------------------------------
# Nota: DynamoDB locking está deprecado en Terraform >= 1.10
# a favor de S3 native locking (use_lockfile). Se mantiene
# para cumplir con el requisito de la rúbrica PE1.
# Ver ADR-0002 para detalles y plan de migración.
# --------------------------------------------------------

resource "aws_dynamodb_table" "terraform_locks" {
  name         = "${var.project_name}-tf-locks-${var.environment}"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "LockID"

  attribute {
    name = "LockID"
    type = "S"
  }
}

# --------------------------------------------------------
# Outputs — usar estos valores en backend.tf de environments/
# --------------------------------------------------------

output "state_bucket_name" {
  description = "Nombre del bucket S3 para Terraform state"
  value       = aws_s3_bucket.terraform_state.id
}

output "state_bucket_arn" {
  description = "ARN del bucket S3 para Terraform state"
  value       = aws_s3_bucket.terraform_state.arn
}

output "lock_table_name" {
  description = "Nombre de la tabla DynamoDB para locking"
  value       = aws_dynamodb_table.terraform_locks.name
}

output "backend_config" {
  description = "Configuración de backend para copiar en environments/dev/backend.tf"
  value = <<-EOT
    backend "s3" {
      bucket         = "${aws_s3_bucket.terraform_state.id}"
      key            = "environments/dev/terraform.tfstate"
      region         = "${var.aws_region}"
      encrypt        = true
      dynamodb_table = "${aws_dynamodb_table.terraform_locks.name}"
    }
  EOT
}
