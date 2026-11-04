# ============================================================
# Módulo Redshift — Outputs
# ============================================================

output "namespace_name" {
  description = "Nombre del namespace Redshift Serverless"
  value       = aws_redshiftserverless_namespace.main.namespace_name
}

output "namespace_id" {
  description = "ID del namespace Redshift Serverless"
  value       = aws_redshiftserverless_namespace.main.id
}

output "workgroup_name" {
  description = "Nombre del workgroup Redshift Serverless"
  value       = aws_redshiftserverless_workgroup.main.workgroup_name
}

output "workgroup_endpoint" {
  description = "Endpoint del workgroup Redshift Serverless"
  value       = aws_redshiftserverless_workgroup.main.endpoint
}

output "database_name" {
  description = "Nombre de la base de datos Redshift"
  value       = var.database_name
}

output "security_group_id" {
  description = "ID del security group de Redshift"
  value       = aws_security_group.redshift.id
}
