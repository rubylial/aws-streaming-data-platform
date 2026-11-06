# ============================================================
# Root Module — Environment Dev
# ============================================================
# Compone todos los módulos de infraestructura
# para el ambiente de desarrollo.
#
# Requisito PE1 (22%): modularización y reutilización.
# Requisito PE2: integración del módulo de ingesta.
# Requisito PE4: integración del módulo analytics (Flink).
# Requisito PE5: integración del módulo lakehouse (Iceberg).
# Requisito PE6: integración del módulo redshift.
# ============================================================

# --------------------------------------------------------
# Módulo Networking (PE1)
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
# Módulo Identity (PE1)
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

# --------------------------------------------------------
# Módulo Ingestion (PE2)
# --------------------------------------------------------
# Kinesis Data Stream + Firehose -> S3 Bronze + KMS + CW
# --------------------------------------------------------

module "ingestion" {
  source = "../../modules/ingestion"

  project_name       = var.project_name
  environment        = var.environment
  firehose_role_arn  = module.identity.firehose_delivery_role_arn
  firehose_role_name = module.identity.firehose_delivery_role_name

  kinesis_shard_count      = var.kinesis_shard_count
  firehose_buffer_size     = var.firehose_buffer_size
  firehose_buffer_interval = var.firehose_buffer_interval

  tags = {
    Component = "ingestion"
  }
}

# --------------------------------------------------------
# Módulo Analytics (PE4)
# --------------------------------------------------------
# Managed Apache Flink — aplicación stateful con checkpoints.
# BLOQUEADO_POR_COSTO: solo declarativo, NO ejecutar apply.
# --------------------------------------------------------

module "analytics" {
  source = "../../modules/analytics"

  project_name        = var.project_name
  environment         = var.environment
  aws_region          = var.aws_region
  kinesis_stream_arn  = module.ingestion.kinesis_stream_arn
  kinesis_stream_name = module.ingestion.kinesis_stream_name
  flink_role_arn      = module.identity.flink_execution_role_arn
  flink_role_name     = module.identity.flink_execution_role_name

  tags = {
    Component = "analytics"
  }
}

# --------------------------------------------------------
# Módulo Lakehouse (PE5)
# --------------------------------------------------------
# Glue Data Catalog + Iceberg tables + S3 lakehouse bucket.
# BLOQUEADO_POR_COSTO: solo declarativo, NO ejecutar apply.
# --------------------------------------------------------

module "lakehouse" {
  source = "../../modules/lakehouse"

  project_name    = var.project_name
  environment     = var.environment
  aws_region      = var.aws_region
  kms_key_arn     = module.ingestion.kms_key_arn
  glue_role_name  = module.identity.glue_service_role_name
  flink_role_name = module.identity.flink_execution_role_name

  tags = {
    Component = "lakehouse"
  }
}

# --------------------------------------------------------
# Módulo Redshift (PE6)
# --------------------------------------------------------
# Redshift Serverless — Streaming Ingestion + Spectrum.
# BLOQUEADO_POR_COSTO: solo declarativo, NO ejecutar apply.
# --------------------------------------------------------

module "redshift" {
  source = "../../modules/redshift"

  project_name         = var.project_name
  environment          = var.environment
  aws_region           = var.aws_region
  vpc_id               = module.networking.vpc_id
  vpc_cidr             = var.vpc_cidr
  private_subnet_ids   = module.networking.private_subnet_ids
  kinesis_stream_arn   = module.ingestion.kinesis_stream_arn
  glue_database_name   = module.lakehouse.glue_database_name
  lakehouse_bucket_arn = module.lakehouse.lakehouse_bucket_arn
  kms_key_arn          = module.ingestion.kms_key_arn
  redshift_role_arn    = module.identity.redshift_service_role_arn
  redshift_role_name   = module.identity.redshift_service_role_name
  admin_user_password  = var.redshift_admin_password

  tags = {
    Component = "redshift"
  }
}