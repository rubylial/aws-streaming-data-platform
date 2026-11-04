# ============================================================
# Variables — Environment Dev
# ============================================================

variable "project_name" {
  description = "Nombre del proyecto para naming y tagging"
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

variable "vpc_cidr" {
  description = "CIDR block para la VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "private_subnet_cidrs" {
  description = "CIDRs para subredes privadas (mínimo 2)"
  type        = list(string)
  default     = ["10.0.1.0/24", "10.0.2.0/24"]
}

variable "availability_zones" {
  description = "AZs para las subredes privadas"
  type        = list(string)
  default     = ["us-east-1a", "us-east-1b"]
}

variable "state_bucket_arn" {
  description = "ARN del bucket S3 de Terraform state (output del bootstrap)"
  type        = string
}

# --- PE2: Ingestion ---

variable "kinesis_shard_count" {
  description = "Número de shards para Kinesis (mínimo 2)"
  type        = number
  default     = 2
}

variable "firehose_buffer_size" {
  description = "Buffer size de Firehose en MB"
  type        = number
  default     = 5
}

variable "firehose_buffer_interval" {
  description = "Buffer interval de Firehose en segundos"
  type        = number
  default     = 60
}
