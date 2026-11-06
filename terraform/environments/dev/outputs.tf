# ============================================================
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

# --- Analytics (PE4) ---

output "flink_application_name" {
  description = "Nombre de la aplicación Managed Flink"
  value       = module.analytics.flink_application_name
}

output "flink_application_arn" {
  description = "ARN de la aplicación Managed Flink"
  value       = module.analytics.flink_application_arn
}

output "flink_artifacts_bucket" {
  description = "Bucket S3 de artefactos Flink"
  value       = module.analytics.flink_artifacts_bucket
}

# --- Lakehouse (PE5) ---

output "lakehouse_bucket_name" {
  description = "Nombre del bucket S3 del lakehouse"
  value       = module.lakehouse.lakehouse_bucket_name
}

output "lakehouse_bucket_arn" {
  description = "ARN del bucket S3 del lakehouse"
  value       = module.lakehouse.lakehouse_bucket_arn
}

output "glue_database_name" {
  description = "Nombre de la base de datos Glue"
  value       = module.lakehouse.glue_database_name
}

output "iceberg_transactions_table" {
  description = "Nombre de la tabla Iceberg de transacciones"
  value       = module.lakehouse.transactions_table_name
}

output "iceberg_aggregations_table" {
  description = "Nombre de la tabla Iceberg de agregaciones"
  value       = module.lakehouse.merchant_aggregations_table_name
}

# --- Redshift (PE6) ---

output "redshift_namespace_name" {
  description = "Nombre del namespace Redshift Serverless"
  value       = module.redshift.namespace_name
}

output "redshift_workgroup_name" {
  description = "Nombre del workgroup Redshift Serverless"
  value       = module.redshift.workgroup_name
}

output "redshift_database_name" {
  description = "Nombre de la base de datos Redshift"
  value       = module.redshift.database_name
}

output "redshift_security_group_id" {
  description = "ID del security group de Redshift"
  value       = module.redshift.security_group_id
}