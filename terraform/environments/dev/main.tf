# ============================================================
# Root Module — Environment Dev
# ============================================================
# Compone los módulos de networking e identity para el
# ambiente de desarrollo. Cada módulo se invoca una sola vez
# con variables del entorno.
#
# Requisito PE1 (22%): modularización y reutilización.
# ============================================================

# --------------------------------------------------------
# Módulo Networking
# --------------------------------------------------------
# Crea: VPC, subredes privadas multi-AZ, route table,
# S3 Gateway Endpoint, SG base.
# --------------------------------------------------------

module "networking" {
  source = "../../modules/networking"

  project_name         = var.project_name
  environment          = var.environment
  vpc_cidr             = var.vpc_cidr
  private_subnet_cidrs = var.private_subnet_cidrs
  availability_zones   = var.availability_zones

  tags = {
    Component = "networking"
  }
}

# --------------------------------------------------------
# Módulo Identity
# --------------------------------------------------------
# Crea: roles IAM segregados por servicio (Firehose, Flink,
# Glue, Redshift) con mínimo privilegio y PassRole controlado.
# --------------------------------------------------------

module "identity" {
  source = "../../modules/identity"

  project_name     = var.project_name
  environment      = var.environment
  aws_region       = var.aws_region
  state_bucket_arn = var.state_bucket_arn

  tags = {
    Component = "identity"
  }
}
