# ============================================================
# Módulo Lakehouse — Glue Data Catalog + S3 + Iceberg
# PE5: tablas Iceberg con catálogo Glue sobre S3
#
# BLOQUEADO_POR_COSTO: módulo declarativo.
# NO ejecutar terraform apply — Glue/S3 pueden generar cargos.
# ============================================================

# --------------------------------------------------------
# S3 Bucket — Lakehouse (datos Iceberg)
# --------------------------------------------------------

resource "aws_s3_bucket" "lakehouse" {
  bucket = "${var.project_name}-lakehouse-${var.environment}"

  tags = merge(var.tags, {
    Component = "lakehouse"
    Service   = "iceberg"
  })
}

resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = var.kms_key_arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  rule {
    id     = "expire-orphan-files"
    status = "Enabled"

    filter {
      prefix = "warehouse/"
    }

    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }
}

# --------------------------------------------------------
# Glue Data Catalog — Database
# --------------------------------------------------------

resource "aws_glue_catalog_database" "lakehouse" {
  name        = "${replace(var.project_name, "-", "_")}_${var.environment}"
  description = "Lakehouse Iceberg — catálogo para tablas de transacciones procesadas por Flink"
  catalog_id  = data.aws_caller_identity.current.account_id

  tags = merge(var.tags, {
    Component = "lakehouse"
  })
}

data "aws_caller_identity" "current" {}

# --------------------------------------------------------
# Glue Table — transactions_iceberg
# --------------------------------------------------------

resource "aws_glue_catalog_table" "transactions" {
  database_name = aws_glue_catalog_database.lakehouse.name
  name          = "transactions"
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "table_type"        = "ICEBERG"
    "format"            = "parquet"
    "write_compression" = "snappy"
    "metadata_location" = "s3://${aws_s3_bucket.lakehouse.id}/warehouse/merchant_aggregations/metadata"  }

  open_table_format_input {
    iceberg_input {
      metadata_operation = "CREATE"
      version            = "2"
    }
  }

  storage_descriptor {
    location = "s3://${aws_s3_bucket.lakehouse.id}/warehouse/transactions"

    columns {
      name = "event_id"
      type = "string"
    }
    columns {
      name = "event_time"
      type = "timestamp"
    }
    columns {
      name = "merchant_id"
      type = "string"
    }
    columns {
      name = "amount"
      type = "double"
    }
    columns {
      name = "currency"
      type = "string"
    }
    columns {
      name = "status"
      type = "string"
    }
    columns {
      name = "processing_time"
      type = "timestamp"
    }
    columns {
      name = "window_start"
      type = "timestamp"
    }
    columns {
      name = "window_end"
      type = "timestamp"
    }
  }
}

# --------------------------------------------------------
# Glue Table — merchant_aggregations_iceberg
# --------------------------------------------------------

resource "aws_glue_catalog_table" "merchant_aggregations" {
  database_name = aws_glue_catalog_database.lakehouse.name
  name          = "merchant_aggregations"
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "table_type"          = "ICEBERG"
    "format"              = "parquet"
    "write_compression"   = "snappy"
    "metadata_location"   = "s3://${aws_s3_bucket.lakehouse.id}/warehouse/merchant_aggregations/metadata"
  }

  open_table_format_input {
    iceberg_input {
      metadata_operation = "CREATE"
      version            = "2"
    }
  }

  storage_descriptor {
    location = "s3://${aws_s3_bucket.lakehouse.id}/warehouse/merchant_aggregations"

    columns {
      name = "merchant_id"
      type = "string"
    }
    columns {
      name = "window_start"
      type = "timestamp"
    }
    columns {
      name = "window_end"
      type = "timestamp"
    }
    columns {
      name = "transaction_count"
      type = "bigint"
    }
    columns {
      name = "total_amount"
      type = "double"
    }
    columns {
      name = "avg_amount"
      type = "double"
    }
  }
}

# --------------------------------------------------------
# IAM — Glue service role policies
# --------------------------------------------------------

# Glue acceso a S3 lakehouse
resource "aws_iam_role_policy" "glue_s3_access" {
  name = "${var.project_name}-glue-s3-lakehouse-${var.environment}"
  role = var.glue_role_name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3LakehouseAccess"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
        ]
        Resource = [
          aws_s3_bucket.lakehouse.arn,
          "${aws_s3_bucket.lakehouse.arn}/*",
        ]
      }
    ]
  })
}

# Glue acceso al catálogo
resource "aws_iam_role_policy" "glue_catalog_access" {
  name = "${var.project_name}-glue-catalog-${var.environment}"
  role = var.glue_role_name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "GlueCatalogAccess"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:UpdateTable",
          "glue:CreateTable",
        ]
        Resource = [
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:catalog",
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:database/${aws_glue_catalog_database.lakehouse.name}",
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.lakehouse.name}/*",
        ]
      }
    ]
  })
}

# Flink acceso a S3 lakehouse (escritura de datos Iceberg)
resource "aws_iam_role_policy" "flink_s3_lakehouse" {
  name = "${var.project_name}-flink-s3-lakehouse-${var.environment}"
  role = var.flink_role_name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "FlinkIcebergS3Access"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation",
        ]
        Resource = [
          aws_s3_bucket.lakehouse.arn,
          "${aws_s3_bucket.lakehouse.arn}/*",
        ]
      }
    ]
  })
}

# Flink acceso al Glue Catalog (para Iceberg catalog)
resource "aws_iam_role_policy" "flink_glue_catalog" {
  name = "${var.project_name}-flink-glue-catalog-${var.environment}"
  role = var.flink_role_name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "FlinkGlueCatalogAccess"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:UpdateTable",
          "glue:CreateTable",
          "glue:DeleteTable",
        ]
        Resource = [
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:catalog",
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:database/${aws_glue_catalog_database.lakehouse.name}",
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.lakehouse.name}/*",
        ]
      }
    ]
  })
}

# KMS decrypt para Flink sobre S3 lakehouse
resource "aws_iam_role_policy" "flink_kms_lakehouse" {
  name = "${var.project_name}-flink-kms-lakehouse-${var.environment}"
  role = var.flink_role_name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "KMSDecryptLakehouse"
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
