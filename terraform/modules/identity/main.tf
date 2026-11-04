# ============================================================
# Módulo Identity — IAM Roles y Policies (Mínimo Privilegio)
# ============================================================
# Requisito PE1 (22%): IAM con mínimo privilegio y segregación.
#
# Roles definidos:
# 1. Firehose Delivery Role — PE2: entregar datos a S3
# 2. Flink Execution Role  — PE4: leer Kinesis, escribir Iceberg/S3
# 3. Glue Service Role     — PE5: catálogo y crawlers
# 4. Redshift Service Role — PE6: leer Kinesis y Glue/S3
#
# Principios:
# - Cada servicio tiene su propio rol (segregación)
# - Policies inline con acciones mínimas necesarias
# - Condición en iam:PassRole para controlar delegación
# - Sin wildcard en Resource salvo donde AWS lo requiere
# - KMS decrypt solo donde se usa cifrado
# ============================================================

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  region     = data.aws_region.current.name
}

# ============================================================
# 1. FIREHOSE DELIVERY ROLE (PE2)
# ============================================================
# Permite a Data Firehose escribir objetos en S3 y descifrar
# con KMS cuando el stream de origen está cifrado.
# ============================================================

resource "aws_iam_role" "firehose_delivery" {
  name = "${var.project_name}-firehose-delivery-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = "firehose.amazonaws.com"
      }
      Action = "sts:AssumeRole"
      Condition = {
        StringEquals = {
          "sts:ExternalId" = local.account_id
        }
      }
    }]
  })

  tags = merge(var.tags, {
    Name    = "${var.project_name}-firehose-delivery-${var.environment}"
    Service = "firehose"
  })
}

resource "aws_iam_role_policy" "firehose_s3_delivery" {
  name = "s3-delivery"
  role = aws_iam_role.firehose_delivery.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3BucketAccess"
        Effect = "Allow"
        Action = [
          "s3:AbortMultipartUpload",
          "s3:GetBucketLocation",
          "s3:GetObject",
          "s3:ListBucket",
          "s3:ListBucketMultipartUploads",
          "s3:PutObject"
        ]
        Resource = [
          "arn:aws:s3:::${var.project_name}-bronze-${var.environment}",
          "arn:aws:s3:::${var.project_name}-bronze-${var.environment}/*"
        ]
      },
      {
        Sid    = "KinesisReadForSource"
        Effect = "Allow"
        Action = [
          "kinesis:DescribeStream",
          "kinesis:GetShardIterator",
          "kinesis:GetRecords",
          "kinesis:ListShards"
        ]
        Resource = "arn:aws:kinesis:${local.region}:${local.account_id}:stream/${var.project_name}-events-${var.environment}"
      }
    ]
  })
}

# ============================================================
# 2. FLINK EXECUTION ROLE (PE4)
# ============================================================
# Permite a Managed Apache Flink:
# - Leer del stream Kinesis
# - Escribir a S3 (tabla Iceberg)
# - Acceder al Glue Data Catalog
# - Escribir logs a CloudWatch
# ============================================================

resource "aws_iam_role" "flink_execution" {
  name = "${var.project_name}-flink-execution-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = "kinesisanalytics.amazonaws.com"
      }
      Action = "sts:AssumeRole"
    }]
  })

  tags = merge(var.tags, {
    Name    = "${var.project_name}-flink-execution-${var.environment}"
    Service = "managed-flink"
  })
}

resource "aws_iam_role_policy" "flink_kinesis_read" {
  name = "kinesis-source-read"
  role = aws_iam_role.flink_execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid    = "KinesisSourceAccess"
      Effect = "Allow"
      Action = [
        "kinesis:DescribeStream",
        "kinesis:GetShardIterator",
        "kinesis:GetRecords",
        "kinesis:ListShards"
      ]
      Resource = "arn:aws:kinesis:${local.region}:${local.account_id}:stream/${var.project_name}-events-${var.environment}"
    }]
  })
}

resource "aws_iam_role_policy" "flink_s3_iceberg" {
  name = "s3-iceberg-readwrite"
  role = aws_iam_role.flink_execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid    = "S3IcebergAccess"
      Effect = "Allow"
      Action = [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ]
      Resource = [
        "arn:aws:s3:::${var.project_name}-lakehouse-${var.environment}",
        "arn:aws:s3:::${var.project_name}-lakehouse-${var.environment}/*"
      ]
    }]
  })
}

resource "aws_iam_role_policy" "flink_glue_catalog" {
  name = "glue-catalog-access"
  role = aws_iam_role.flink_execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid    = "GlueCatalogAccess"
      Effect = "Allow"
      Action = [
        "glue:GetDatabase",
        "glue:GetDatabases",
        "glue:GetTable",
        "glue:GetTables",
        "glue:GetPartitions",
        "glue:CreateTable",
        "glue:UpdateTable",
        "glue:GetUserDefinedFunctions"
      ]
      Resource = [
        "arn:aws:glue:${local.region}:${local.account_id}:catalog",
        "arn:aws:glue:${local.region}:${local.account_id}:database/${var.project_name}_${var.environment}",
        "arn:aws:glue:${local.region}:${local.account_id}:table/${var.project_name}_${var.environment}/*"
      ]
    }]
  })
}

resource "aws_iam_role_policy" "flink_cloudwatch_logs" {
  name = "cloudwatch-logs"
  role = aws_iam_role.flink_execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid    = "CloudWatchLogs"
      Effect = "Allow"
      Action = [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents",
        "logs:DescribeLogStreams"
      ]
      Resource = "arn:aws:logs:${local.region}:${local.account_id}:log-group:/aws/kinesis-analytics/${var.project_name}-*"
    }]
  })
}

# ============================================================
# 3. GLUE SERVICE ROLE (PE5)
# ============================================================

resource "aws_iam_role" "glue_service" {
  name = "${var.project_name}-glue-service-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = "glue.amazonaws.com"
      }
      Action = "sts:AssumeRole"
    }]
  })

  tags = merge(var.tags, {
    Name    = "${var.project_name}-glue-service-${var.environment}"
    Service = "glue"
  })
}

resource "aws_iam_role_policy_attachment" "glue_service_base" {
  role       = aws_iam_role.glue_service.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3_lakehouse" {
  name = "s3-lakehouse-access"
  role = aws_iam_role.glue_service.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid    = "S3LakehouseAccess"
      Effect = "Allow"
      Action = [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ]
      Resource = [
        "arn:aws:s3:::${var.project_name}-lakehouse-${var.environment}",
        "arn:aws:s3:::${var.project_name}-lakehouse-${var.environment}/*"
      ]
    }]
  })
}

# ============================================================
# 4. REDSHIFT SERVICE ROLE (PE6)
# ============================================================
# Permite a Redshift:
# - Leer del stream Kinesis (Streaming Ingestion)
# - Acceder al Glue Data Catalog (External Schema)
# - Leer S3 para Iceberg vía Spectrum
# ============================================================

resource "aws_iam_role" "redshift_service" {
  name = "${var.project_name}-redshift-service-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = "redshift.amazonaws.com"
      }
      Action = "sts:AssumeRole"
    }]
  })

  tags = merge(var.tags, {
    Name    = "${var.project_name}-redshift-service-${var.environment}"
    Service = "redshift"
  })
}

resource "aws_iam_role_policy" "redshift_kinesis_read" {
  name = "kinesis-streaming-ingestion"
  role = aws_iam_role.redshift_service.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid    = "KinesisStreamingIngestion"
      Effect = "Allow"
      Action = [
        "kinesis:DescribeStream",
        "kinesis:GetShardIterator",
        "kinesis:GetRecords",
        "kinesis:DescribeStreamSummary",
        "kinesis:ListShards"
      ]
      Resource = "arn:aws:kinesis:${local.region}:${local.account_id}:stream/${var.project_name}-events-${var.environment}"
    }]
  })
}

resource "aws_iam_role_policy" "redshift_glue_catalog" {
  name = "glue-catalog-spectrum"
  role = aws_iam_role.redshift_service.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid    = "GlueCatalogForSpectrum"
      Effect = "Allow"
      Action = [
        "glue:GetDatabase",
        "glue:GetDatabases",
        "glue:GetTable",
        "glue:GetTables",
        "glue:GetPartitions"
      ]
      Resource = [
        "arn:aws:glue:${local.region}:${local.account_id}:catalog",
        "arn:aws:glue:${local.region}:${local.account_id}:database/${var.project_name}_${var.environment}",
        "arn:aws:glue:${local.region}:${local.account_id}:table/${var.project_name}_${var.environment}/*"
      ]
    }]
  })
}

resource "aws_iam_role_policy" "redshift_s3_spectrum" {
  name = "s3-spectrum-read"
  role = aws_iam_role.redshift_service.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid    = "S3SpectrumRead"
      Effect = "Allow"
      Action = [
        "s3:GetObject",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ]
      Resource = [
        "arn:aws:s3:::${var.project_name}-lakehouse-${var.environment}",
        "arn:aws:s3:::${var.project_name}-lakehouse-${var.environment}/*"
      ]
    }]
  })
}

# ============================================================
# 5. PASSROLE POLICY — controla delegación de roles
# ============================================================
# Restringe qué principal puede pasar cada rol a su servicio.
# Esto evita escalamiento de privilegios vía PassRole.
# ============================================================

resource "aws_iam_policy" "passrole_control" {
  name        = "${var.project_name}-passrole-control-${var.environment}"
  description = "Controla qué roles pueden ser pasados y a qué servicios"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "AllowPassRoleToFirehose"
        Effect   = "Allow"
        Action   = "iam:PassRole"
        Resource = aws_iam_role.firehose_delivery.arn
        Condition = {
          StringEquals = {
            "iam:PassedToService" = "firehose.amazonaws.com"
          }
        }
      },
      {
        Sid      = "AllowPassRoleToFlink"
        Effect   = "Allow"
        Action   = "iam:PassRole"
        Resource = aws_iam_role.flink_execution.arn
        Condition = {
          StringEquals = {
            "iam:PassedToService" = "kinesisanalytics.amazonaws.com"
          }
        }
      },
      {
        Sid      = "AllowPassRoleToGlue"
        Effect   = "Allow"
        Action   = "iam:PassRole"
        Resource = aws_iam_role.glue_service.arn
        Condition = {
          StringEquals = {
            "iam:PassedToService" = "glue.amazonaws.com"
          }
        }
      },
      {
        Sid      = "AllowPassRoleToRedshift"
        Effect   = "Allow"
        Action   = "iam:PassRole"
        Resource = aws_iam_role.redshift_service.arn
        Condition = {
          StringEquals = {
            "iam:PassedToService" = "redshift.amazonaws.com"
          }
        }
      }
    ]
  })

  tags = merge(var.tags, {
    Name = "${var.project_name}-passrole-control-${var.environment}"
  })
}
