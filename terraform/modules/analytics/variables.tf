# ============================================================
# Módulo Analytics — Variables
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

# --- Kinesis (inputs del módulo ingestion) ---

variable "kinesis_stream_arn" {
  description = "ARN del Kinesis Data Stream a consumir"
  type        = string
}

variable "kinesis_stream_name" {
  description = "Nombre del Kinesis Data Stream"
  type        = string
}

# --- IAM (inputs del módulo identity) ---

variable "flink_role_arn" {
  description = "ARN del rol de ejecución de Flink"
  type        = string
}

variable "flink_role_name" {
  description = "Nombre del rol de ejecución de Flink"
  type        = string
}

# --- Configuración ---

variable "log_retention_days" {
  description = "Días de retención de logs CloudWatch"
  type        = number
  default     = 7
}

variable "tags" {
  description = "Tags comunes"
  type        = map(string)
  default     = {}
}
