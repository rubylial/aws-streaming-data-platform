# ============================================================
# Módulo Networking — VPC, Subredes Privadas, S3 Gateway Endpoint
# ============================================================
# Requisito PE1 (28%): VPC privada con al menos 2 subredes en
# AZs distintas y S3 Gateway Endpoint.
#
# Decisiones:
# - Solo subredes privadas: los servicios AWS se alcanzan vía
#   VPC endpoints (S3 Gateway aquí; Kinesis/Glue/STS/Logs
#   como Interface endpoints en módulos posteriores si se
#   justifica) o NAT Gateway (costo alto — ver D-03).
# - NO se crea NAT Gateway por política de costo cero.
# - NO se crean subredes públicas (no hay requisito).
# ============================================================

# --------------------------------------------------------
# VPC
# --------------------------------------------------------

resource "aws_vpc" "main" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = merge(var.tags, {
    Name = "${var.project_name}-vpc-${var.environment}"
  })
}

# --------------------------------------------------------
# Subredes Privadas (mínimo 2, en AZs distintas)
# --------------------------------------------------------

resource "aws_subnet" "private" {
  count = length(var.private_subnet_cidrs)

  vpc_id            = aws_vpc.main.id
  cidr_block        = var.private_subnet_cidrs[count.index]
  availability_zone = var.availability_zones[count.index]

  # Sin IP pública — son subredes privadas
  map_public_ip_on_launch = false

  tags = merge(var.tags, {
    Name = "${var.project_name}-private-${var.availability_zones[count.index]}-${var.environment}"
    Tier = "private"
  })
}

# --------------------------------------------------------
# Route Table para subredes privadas
# --------------------------------------------------------

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.main.id

  tags = merge(var.tags, {
    Name = "${var.project_name}-rt-private-${var.environment}"
  })
}

resource "aws_route_table_association" "private" {
  count = length(aws_subnet.private)

  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private.id
}

# --------------------------------------------------------
# S3 Gateway Endpoint (requisito PE1 — sin costo)
# --------------------------------------------------------
# Los Gateway Endpoints para S3 y DynamoDB no tienen cargo.
# Permiten tráfico directo desde subredes privadas a S3
# sin necesidad de NAT Gateway ni Internet Gateway.
# --------------------------------------------------------

resource "aws_vpc_endpoint" "s3" {
  vpc_id       = aws_vpc.main.id
  service_name = "com.amazonaws.${data.aws_region.current.name}.s3"

  vpc_endpoint_type = "Gateway"

  route_table_ids = [aws_route_table.private.id]

  tags = merge(var.tags, {
    Name = "${var.project_name}-vpce-s3-${var.environment}"
  })
}

# --------------------------------------------------------
# Data sources
# --------------------------------------------------------

data "aws_region" "current" {}

# --------------------------------------------------------
# Security Group base (restrictivo por defecto)
# --------------------------------------------------------
# SG para recursos dentro de la VPC. Solo permite tráfico
# de salida HTTPS (443) hacia el prefijo de S3 vía endpoint.
# Reglas específicas por servicio se agregan en módulos
# posteriores (Kinesis, Flink, Redshift).
# --------------------------------------------------------

resource "aws_security_group" "internal" {
  name_prefix = "${var.project_name}-internal-"
  description = "SG base para recursos internos de la plataforma"
  vpc_id      = aws_vpc.main.id

  # Tráfico de salida HTTPS (para VPC endpoints y S3)
  egress {
    description = "HTTPS outbound para endpoints AWS"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  # S3 Gateway endpoint usa prefix list, no CIDR
  egress {
    description     = "S3 via Gateway Endpoint"
    from_port       = 443
    to_port         = 443
    protocol        = "tcp"
    prefix_list_ids = [aws_vpc_endpoint.s3.prefix_list_id]
  }

  tags = merge(var.tags, {
    Name = "${var.project_name}-sg-internal-${var.environment}"
  })

  lifecycle {
    create_before_destroy = true
  }
}
