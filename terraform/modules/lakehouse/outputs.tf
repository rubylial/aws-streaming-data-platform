# ============================================================
# Módulo Lakehouse — Outputs
# ============================================================

output "lakehouse_bucket_name" {
  description = "Nombre del bucket S3 del lakehouse"
  value       = aws_s3_bucket.lakehouse.id
}

output "lakehouse_bucket_arn" {
  description = "ARN del bucket S3 del lakehouse"
  value       = aws_s3_bucket.lakehouse.arn
}

output "glue_database_name" {
  description = "Nombre de la base de datos Glue"
  value       = aws_glue_catalog_database.lakehouse.name
}

output "glue_database_arn" {
  description = "ARN de la base de datos Glue"
  value       = aws_glue_catalog_database.lakehouse.arn
}

output "transactions_table_name" {
  description = "Nombre de la tabla Iceberg de transacciones"
  value       = aws_glue_catalog_table.transactions.name
}

output "merchant_aggregations_table_name" {
  description = "Nombre de la tabla Iceberg de agregaciones"
  value       = aws_glue_catalog_table.merchant_aggregations.name
}
