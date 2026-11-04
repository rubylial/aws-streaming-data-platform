# =============================================================
# Outputs — Environment Dev
# ============================================================

# --- Networking (PE1) ---

output "vpc_id" {
  description = "ID de la VPC"
  value       = module.networking.vpc_id
}

output "private_subnet_ids" {
  description = "IDs de las subredes privadas"
  value       = module.networking.private_subnet_ids
}

output "s3_endpoint_id" {
  description = "ID del S3 Gateway VPC Endpoint"
  value       = module.networking.s3_endpoint_id
}

output "internal_security_group_id" {
  description = "ID del SG base interno"
  value       = module.networking.internal_security_group_id
}

# --- Identity (PE1) ---

output "firehose_delivery_role_arn" {
  description = "ARN del rol de Firehose"
  value       = module.identity.firehose_delivery_role_arn
}

output "flink_execution_role_arn" {
  description = "ARN del rol de Flink"
  value       = module.identity.flink_execution_role_arn
}

output "glue_service_role_arn" {
  description = "ARN del rol de Glue"
  value       = module.identity.glue_service_role_arn
}

output "redshift_service_role_arn" {
  description = "ARN del rol de Redshift"
  value       = module.identity.redshift_service_role_arn
}

# --- Ingestion (PE2) ---

output "kinesis_stream_name" {
  description = "Nombre del Kinesis Data Stream"
  value       = module.ingestion.kinesis_stream_name
}

output "kinesis_stream_arn" {
  description = "ARN del Kinesis Data Stream"
  value       = module.ingestion.kinesis_stream_arn
}

output "firehose_delivery_stream_name" {
  description = "Nombre del Firehose Delivery Stream"
  value       = module.ingestion.firehose_delivery_stream_name
}

output "bronze_bucket_name" {
  description = "Nombre del bucket S3 bronze"
  value       = module.ingestion.bronze_bucket_name
}

output "kms_key_arn" {
  description = "ARN de la CMK de streaming"
  value       = module.ingestion.kms_key_arn
}
