# ============================================================
# Módulo Analytics — Outputs
# ============================================================

output "flink_application_name" {
  description = "Nombre de la aplicación Managed Flink"
  value       = aws_kinesisanalyticsv2_application.flink_app.name
}

output "flink_application_arn" {
  description = "ARN de la aplicación Managed Flink"
  value       = aws_kinesisanalyticsv2_application.flink_app.arn
}

output "flink_artifacts_bucket" {
  description = "Nombre del bucket S3 de artefactos Flink"
  value       = aws_s3_bucket.flink_artifacts.id
}

output "flink_artifacts_bucket_arn" {
  description = "ARN del bucket S3 de artefactos Flink"
  value       = aws_s3_bucket.flink_artifacts.arn
}

output "flink_log_group_name" {
  description = "Nombre del log group de Flink"
  value       = aws_cloudwatch_log_group.flink.name
}
