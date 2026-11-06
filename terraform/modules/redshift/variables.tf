# ============================================================
# Módulo Redshift — Variables
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

# --- Redshift config ---

variable "database_name" {
  description = "Nombre de la base de datos Redshift"
  type        = string
  default     = "streaming_analytics"
}

variable "admin_username" {
  description = "Usuario admin de Redshift"
  type        = string
  default     = "admin"
  sensitive   = true
}

variable "admin_user_password" {
  description = "Contraseña del usuario admin"
  type        = string
  sensitive   = true
}

variable "base_capacity_rpu" {
  description = "Capacidad base en RPU (Redshift Processing Units)"
  type        = number
  default     = 8
}

# --- Networking ---

variable "vpc_id" {
  description = "ID de la VPC"
  type        = string
}

variable "vpc_cidr" {
  description = "CIDR de la VPC"
  type        = string
}

variable "private_subnet_ids" {
  description = "IDs de subnets privadas"
  type        = list(string)
}

# --- Kinesis ---

variable "kinesis_stream_arn" {
  description = "ARN del Kinesis Data Stream"
  type        = string
}

# --- Glue / Lakehouse ---

variable "glue_database_name" {
  description = "Nombre de la base de datos Glue (para Spectrum)"
  type        = string
}

variable "lakehouse_bucket_arn" {
  description = "ARN del bucket S3 del lakehouse (Iceberg)"
  type        = string
}

# --- KMS ---

variable "kms_key_arn" {
  description = "ARN de la llave KMS"
  type        = string
}

# --- IAM ---

variable "redshift_role_arn" {
  description = "ARN del rol IAM de Redshift"
  type        = string
}

variable "redshift_role_name" {
  description = "Nombre del rol IAM de Redshift"
  type        = string
}

# --- Tags ---

variable "tags" {
  description = "Tags comunes"
  type        = map(string)
  default     = {}
}
