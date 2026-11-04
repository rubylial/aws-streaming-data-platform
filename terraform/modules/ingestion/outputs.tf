# ============================================================
# Módulo Ingestion — Outputs
# ============================================================

# --- Kinesis ---

output "kinesis_stream_name" {
  description = "Nombre del Kinesis Data Stream"
  value       = aws_kinesis_stream.events.name
}

output "kinesis_stream_arn" {
  description = "ARN del Kinesis Data Stream"
  value       = aws_kinesis_stream.events.arn
}

output "kinesis_shard_count" {
  description = "Número de shards configurados"
  value       = aws_kinesis_stream.events.shard_count
}

# --- Firehose ---

output "firehose_delivery_stream_name" {
  description = "Nombre del Firehose Delivery Stream"
  value       = aws_kinesis_firehose_delivery_stream.s3_bronze.name
}

output "firehose_delivery_stream_arn" {
  description = "ARN del Firehose Delivery Stream"
  value       = aws_kinesis_firehose_delivery_stream.s3_bronze.arn
}

# --- S3 Bronze ---

output "bronze_bucket_name" {
  description = "Nombre del bucket S3 bronze"
  value       = aws_s3_bucket.bronze.id
}

output "bronze_bucket_arn" {
  description = "ARN del bucket S3 bronze"
  value       = aws_s3_bucket.bronze.arn
}

# --- KMS ---

output "kms_key_arn" {
  description = "ARN de la CMK de streaming"
  value       = aws_kms_key.streaming.arn
}

output "kms_key_id" {
  description = "ID de la CMK de streaming"
  value       = aws_kms_key.streaming.key_id
}

# --- CloudWatch ---

output "firehose_log_group_name" {
  description = "Nombre del log group de Firehose"
  value       = aws_cloudwatch_log_group.firehose.name
}
