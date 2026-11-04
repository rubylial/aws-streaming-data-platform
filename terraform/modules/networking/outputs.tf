# ============================================================
# Módulo Networking — Outputs
# ============================================================

output "vpc_id" {
  description = "ID de la VPC"
  value       = aws_vpc.main.id
}

output "vpc_cidr" {
  description = "CIDR block de la VPC"
  value       = aws_vpc.main.cidr_block
}

output "private_subnet_ids" {
  description = "IDs de las subredes privadas"
  value       = aws_subnet.private[*].id
}

output "private_subnet_cidrs" {
  description = "CIDRs de las subredes privadas"
  value       = aws_subnet.private[*].cidr_block
}

output "private_subnet_azs" {
  description = "AZs de las subredes privadas"
  value       = aws_subnet.private[*].availability_zone
}

output "private_route_table_id" {
  description = "ID de la route table privada"
  value       = aws_route_table.private.id
}

output "s3_endpoint_id" {
  description = "ID del S3 Gateway VPC Endpoint"
  value       = aws_vpc_endpoint.s3.id
}

output "s3_endpoint_prefix_list_id" {
  description = "Prefix list ID del S3 endpoint (para SG rules)"
  value       = aws_vpc_endpoint.s3.prefix_list_id
}

output "internal_security_group_id" {
  description = "ID del SG base interno"
  value       = aws_security_group.internal.id
}
