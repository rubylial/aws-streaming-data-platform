# ============================================================
# Módulo Identity — Variables
# ============================================================

variable "project_name" {
  description = "Nombre del proyecto para naming y tagging"
  type        = string
}

variable "environment" {
  description = "Ambiente (dev, staging, prod)"
  type        = string
}

variable "aws_region" {
  description = "Región AWS"
  type        = string
}

variable "state_bucket_arn" {
  description = "ARN del bucket S3 de Terraform state (para policy del rol CI/CD)"
  type        = string
}

variable "tags" {
  description = "Tags adicionales para todos los recursos del módulo"
  type        = map(string)
  default     = {}
}
