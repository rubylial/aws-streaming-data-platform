# ============================================================
# Módulo Lakehouse — Variables
# ============================================================

variable "project_name" {
  description = "Nombre del proyecto"
  type        = string
}

variable "environment" {
  description = "Ambiente (dev, staging, prod)"
  type        = string
}

variable "aws_region" {
  description = "Región AWS"
  type        = string
  default     = "us-east-1"
}

# --- KMS ---

variable "kms_key_arn" {
  description = "ARN de la llave KMS para cifrado S3"
  type        = string
}

# --- IAM (inputs de identity module) ---

variable "glue_role_name" {
  description = "Nombre del rol IAM de Glue"
  type        = string
}

variable "flink_role_name" {
  description = "Nombre del rol IAM de Flink (para acceso Iceberg)"
  type        = string
}

# --- Tags ---

variable "tags" {
  description = "Tags comunes"
  type        = map(string)
  default     = {}
}
