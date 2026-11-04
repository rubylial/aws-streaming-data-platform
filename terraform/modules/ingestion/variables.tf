# ============================================================
# Módulo Ingestion — Variables
# ============================================================

variable "project_name" {
  description = "Nombre del proyecto para naming de recursos"
  type        = string
}

variable "environment" {
  description = "Ambiente (dev, staging, prod)"
  type        = string
}

# --- Kinesis ---

variable "kinesis_shard_count" {
  description = "Número de shards para Kinesis Data Stream (mínimo 2 por rúbrica PE2)"
  type        = number
  default     = 2

  validation {
    condition     = var.kinesis_shard_count >= 2
    error_message = "PE2 requiere al menos 2 shards en modo PROVISIONED."
  }
}

variable "kinesis_retention_hours" {
  description = "Período de retención del stream en horas"
  type        = number
  default     = 24

  validation {
    condition     = var.kinesis_retention_hours >= 24 && var.kinesis_retention_hours <= 8760
    error_message = "La retención debe estar entre 24 y 8760 horas."
  }
}

# --- Firehose ---

variable "firehose_buffer_size" {
  description = "Tamaño del buffer de Firehose en MB (rúbrica PE2: 5 MB)"
  type        = number
  default     = 5

  validation {
    condition     = var.firehose_buffer_size >= 1 && var.firehose_buffer_size <= 128
    error_message = "El buffer debe estar entre 1 y 128 MB."
  }
}

variable "firehose_buffer_interval" {
  description = "Intervalo del buffer de Firehose en segundos (rúbrica PE2: 60 s)"
  type        = number
  default     = 60

  validation {
    condition     = var.firehose_buffer_interval >= 60 && var.firehose_buffer_interval <= 900
    error_message = "El intervalo debe estar entre 60 y 900 segundos."
  }
}

# --- IAM (del módulo identity) ---

variable "firehose_role_arn" {
  description = "ARN del rol de Firehose (output del módulo identity)"
  type        = string
}

variable "firehose_role_name" {
  description = "Nombre del rol de Firehose (output del módulo identity)"
  type        = string
}

# --- KMS ---

variable "kms_deletion_window" {
  description = "Días de espera antes de eliminar la CMK"
  type        = number
  default     = 7

  validation {
    condition     = var.kms_deletion_window >= 7 && var.kms_deletion_window <= 30
    error_message = "El período debe estar entre 7 y 30 días."
  }
}

# --- CloudWatch ---

variable "log_retention_days" {
  description = "Días de retención de logs de CloudWatch"
  type        = number
  default     = 7
}

# --- Tags ---

variable "tags" {
  description = "Tags comunes para los recursos"
  type        = map(string)
  default     = {}
}
