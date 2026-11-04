# ============================================================
# Módulo Identity — Outputs
# ============================================================

# --- Firehose ---

output "firehose_delivery_role_arn" {
  description = "ARN del rol de entrega de Firehose"
  value       = aws_iam_role.firehose_delivery.arn
}

output "firehose_delivery_role_name" {
  description = "Nombre del rol de entrega de Firehose"
  value       = aws_iam_role.firehose_delivery.name
}

# --- Flink ---

output "flink_execution_role_arn" {
  description = "ARN del rol de ejecución de Managed Flink"
  value       = aws_iam_role.flink_execution.arn
}

output "flink_execution_role_name" {
  description = "Nombre del rol de ejecución de Managed Flink"
  value       = aws_iam_role.flink_execution.name
}

# --- Glue ---

output "glue_service_role_arn" {
  description = "ARN del rol de servicio de Glue"
  value       = aws_iam_role.glue_service.arn
}

output "glue_service_role_name" {
  description = "Nombre del rol de servicio de Glue"
  value       = aws_iam_role.glue_service.name
}

# --- Redshift ---

output "redshift_service_role_arn" {
  description = "ARN del rol de servicio de Redshift"
  value       = aws_iam_role.redshift_service.arn
}

output "redshift_service_role_name" {
  description = "Nombre del rol de servicio de Redshift"
  value       = aws_iam_role.redshift_service.name
}

# --- PassRole ---

output "passrole_policy_arn" {
  description = "ARN de la policy de control de PassRole"
  value       = aws_iam_policy.passrole_control.arn
}
