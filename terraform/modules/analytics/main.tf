# ============================================================
# Módulo Analytics — Managed Apache Flink
# PE4: aplicación Flink stateful con checkpoints
#
# BLOQUEADO_POR_COSTO: este módulo es declarativo.
# NO ejecutar terraform apply — Managed Flink genera cargos.
# ============================================================

# --------------------------------------------------------
# S3 Bucket para artefactos Flink (JAR)
# --------------------------------------------------------

resource "aws_s3_bucket" "flink_artifacts" {
  bucket = "${var.project_name}-flink-artifacts-${var.environment}"

  tags = merge(var.tags, {
    Component = "analytics"
    Service   = "flink"
  })
}

resource "aws_s3_bucket_versioning" "flink_artifacts" {
  bucket = aws_s3_bucket.flink_artifacts.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "flink_artifacts" {
  bucket = aws_s3_bucket.flink_artifacts.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# --------------------------------------------------------
# CloudWatch Log Group para Flink
# --------------------------------------------------------

resource "aws_cloudwatch_log_group" "flink" {
  name              = "/aws/kinesis-analytics/${var.project_name}-${var.environment}"
  retention_in_days = var.log_retention_days

  tags = merge(var.tags, {
    Component = "analytics"
  })
}

resource "aws_cloudwatch_log_stream" "flink" {
  name           = "flink-application"
  log_group_name = aws_cloudwatch_log_group.flink.name
}

# --------------------------------------------------------
# IAM Policies adicionales para Flink
# --------------------------------------------------------

# Acceso de Flink al stream Kinesis (lectura)
resource "aws_iam_role_policy" "flink_kinesis_access" {
  name = "${var.project_name}-flink-kinesis-access-${var.environment}"
  role = var.flink_role_name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "KinesisReadAccess"
        Effect = "Allow"
        Action = [
          "kinesis:DescribeStream",
          "kinesis:DescribeStreamSummary",
          "kinesis:GetRecords",
          "kinesis:GetShardIterator",
          "kinesis:ListShards",
          "kinesis:ListStreams",
          "kinesis:SubscribeToShard",
        ]
        Resource = var.kinesis_stream_arn
      }
    ]
  })
}

# Acceso de Flink a S3 (artefactos y checkpoints)
resource "aws_iam_role_policy" "flink_s3_access" {
  name = "${var.project_name}-flink-s3-access-${var.environment}"
  role = var.flink_role_name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3ArtifactAccess"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket",
        ]
        Resource = [
          aws_s3_bucket.flink_artifacts.arn,
          "${aws_s3_bucket.flink_artifacts.arn}/*",
        ]
      }
    ]
  })
}

# Acceso de Flink a CloudWatch Logs
resource "aws_iam_role_policy" "flink_cloudwatch" {
  name = "${var.project_name}-flink-cw-logs-${var.environment}"
  role = var.flink_role_name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "CloudWatchLogsAccess"
        Effect = "Allow"
        Action = [
          "logs:PutLogEvents",
          "logs:DescribeLogGroups",
          "logs:DescribeLogStreams",
        ]
        Resource = "${aws_cloudwatch_log_group.flink.arn}:*"
      }
    ]
  })
}

# --------------------------------------------------------
# Managed Apache Flink Application
# --------------------------------------------------------

resource "aws_kinesisanalyticsv2_application" "flink_app" {
  name                   = "${var.project_name}-flink-${var.environment}"
  runtime_environment    = "FLINK-1_19"
  service_execution_role = var.flink_role_arn

  application_configuration {
    application_code_configuration {
      code_content {
        s3_content_location {
          bucket_arn = aws_s3_bucket.flink_artifacts.arn
          file_key   = "flink-streaming-job-1.0.0.jar"
        }
      }
      code_content_type = "ZIPFILE"
    }

    environment_properties {
      property_group {
        property_group_id = "FlinkApplicationProperties"
        property_map = {
          "stream.name"         = var.kinesis_stream_name
          "aws.region"          = var.aws_region
          "checkpoint.interval" = "60000"
        }
      }
    }

    flink_application_configuration {
      checkpoint_configuration {
        configuration_type            = "CUSTOM"
        checkpointing_enabled         = true
        checkpoint_interval           = 60000
        min_pause_between_checkpoints = 30000
      }

      monitoring_configuration {
        configuration_type = "CUSTOM"
        log_level          = "INFO"
        metrics_level      = "APPLICATION"
      }

      parallelism_configuration {
        configuration_type   = "CUSTOM"
        parallelism          = 2
        parallelism_per_kpu  = 1
        auto_scaling_enabled = false
      }
    }
  }

  cloudwatch_logging_options {
    log_stream_arn = aws_cloudwatch_log_stream.flink.arn
  }

  tags = merge(var.tags, {
    Component = "analytics"
    Service   = "managed-flink"
  })
}
