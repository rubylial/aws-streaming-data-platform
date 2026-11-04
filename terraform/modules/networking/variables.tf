# ============================================================
# Módulo Networking — Variables
# ============================================================

variable "project_name" {
  description = "Nombre del proyecto para naming y tagging"
  type        = string
}

variable "environment" {
  description = "Ambiente (dev, staging, prod)"
  type        = string
}

variable "vpc_cidr" {
  description = "CIDR block para la VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "private_subnet_cidrs" {
  description = "Lista de CIDRs para subredes privadas (mínimo 2, en AZs distintas)"
  type        = list(string)
  default     = ["10.0.1.0/24", "10.0.2.0/24"]

  validation {
    condition     = length(var.private_subnet_cidrs) >= 2
    error_message = "Se requieren al menos 2 subredes privadas en AZs distintas (requisito PE1)."
  }
}

variable "availability_zones" {
  description = "Lista de AZs para las subredes (debe coincidir con private_subnet_cidrs)"
  type        = list(string)
  default     = ["us-east-1a", "us-east-1b"]

  validation {
    condition     = length(var.availability_zones) >= 2
    error_message = "Se requieren al menos 2 AZs distintas (requisito PE1)."
  }
}

variable "tags" {
  description = "Tags adicionales para todos los recursos del módulo"
  type        = map(string)
  default     = {}
}
