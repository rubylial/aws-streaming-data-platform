# ============================================================
# Módulo Redshift — Serverless Namespace + Workgroup
# PE6: Streaming Ingestion desde Kinesis + consulta Iceberg
#
# BLOQUEADO_POR_COSTO: módulo declarativo.
# NO ejecutar terraform apply — Redshift Serverless genera cargos.
# ============================================================

# --------------------------------------------------------
# Redshift Serverless Namespace
# --------------------------------------------------------

resource "aws_redshiftserverless_namespace" "main" {
  namespace_name      = "${var.project_name}-${var.environment}"
  db_name             = var.database_name
  admin_username      = var.admin_username
  admin_user_password = var.admin_user_password

  iam_roles = [var.redshift_role_arn]

  default_iam_role_arn = var.redshift_role_arn

  log_exports = ["userlog", "connectionlog", "useractivitylog"]

  tags = merge(var.tags, {
    Component = "analytics"
    Service   = "redshift-serverless"
  })
}

# --------------------------------------------------------
# Redshift Serverless Workgroup
# --------------------------------------------------------

resource "aws_redshiftserverless_workgroup" "main" {
  namespace_name = aws_redshiftserverless_namespace.main.namespace_name
  workgroup_name = "${var.project_name}-wg-${var.environment}"

  base_capacity = var.base_capacity_rpu

  publicly_accessible = false

  subnet_ids         = var.private_subnet_ids
  security_group_ids = [aws_security_group.redshift.id]

  config_parameter {
    parameter_key   = "auto_mv"
    parameter_value = "true"
  }

  config_parameter {
    parameter_key   = "enable_case_sensitive_identifier"
    parameter_value = "true"
  }

  tags = merge(var.tags, {
    Component = "analytics"
    Service   = "redshift-serverless"
  })
}

# --------------------------------------------------------
# Security Group para Redshift
# --------------------------------------------------------

resource "aws_security_group" "redshift" {
  name_prefix = "${var.project_name}-redshift-${var.environment}-"
  vpc_id      = var.vpc_id
  description = "Security group para Redshift Serverless"

  ingress {
    description = "Redshift port desde VPC"
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  egress {
    description = "All outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.tags, {
    Component = "analytics"
    Name      = "${var.project_name}-redshift-sg-${var.environment}"
  })

  lifecycle {
    create_before_destroy = true
  }
}

# --------------------------------------------------------
# IAM — Redshift acceso a Kinesis (Streaming Ingestion)
# --------------------------------------------------------

resource "aws_iam_role_policy" "redshift_kinesis_access" {
  name = "${var.project_name}-redshift-kinesis-${var.environment}"
  role = var.redshift_role_name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "KinesisStreamingIngestion"
        Effect = "Allow"
        Action = [
          "kinesis:DescribeStream",
          "kinesis:DescribeStreamSummary",
          "kinesis:GetRecords",
          "kinesis:GetShardIterator",
          "kinesis:ListShards",
          "kinesis:SubscribeToShard",
          "kinesis:DescribeStreamConsumer",
          "kinesis:RegisterStreamConsumer",
          "kinesis:DeregisterStreamConsumer",
          "kinesis:ListStreamConsumers",
        ]
        Resource = [
          var.kinesis_stream_arn,
          "${var.kinesis_stream_arn}/*",
        ]
      }
    ]
  })
}

# --------------------------------------------------------
# IAM — Redshift acceso a Glue Data Catalog (Spectrum)
# --------------------------------------------------------

resource "aws_iam_role_policy" "redshift_glue_access" {
  name = "${var.project_name}-redshift-glue-${var.environment}"
  role = var.redshift_role_name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "GlueCatalogReadAccess"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetPartition",
          "glue:GetPartitions",
        ]
        Resource = [
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:catalog",
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:database/${var.glue_database_name}",
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/${var.glue_database_name}/*",
        ]
      }
    ]
  })
}

# --------------------------------------------------------
# IAM — Redshift acceso a S3 Lakehouse (Spectrum)
# --------------------------------------------------------

resource "aws_iam_role_policy" "redshift_s3_lakehouse" {
  name = "${var.project_name}-redshift-s3-lakehouse-${var.environment}"
  role = var.redshift_role_name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3LakehouseReadAccess"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
          "s3:GetBucketLocation",
        ]
        Resource = [
          var.lakehouse_bucket_arn,
          "${var.lakehouse_bucket_arn}/*",
        ]
      }
    ]
  })
}

# --------------------------------------------------------
# IAM — Redshift KMS (descifrado S3)
# --------------------------------------------------------

resource "aws_iam_role_policy" "redshift_kms" {
  name = "${var.project_name}-redshift-kms-${var.environment}"
  role = var.redshift_role_name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "KMSDecrypt"
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:GenerateDataKey",
        ]
        Resource = var.kms_key_arn
      }
    ]
  })
}

data "aws_caller_identity" "current" {}
