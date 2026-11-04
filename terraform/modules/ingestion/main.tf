# ============================================================
# Módulo Ingestion — Kinesis, Firehose, S3 Bronze, KMS
# ============================================================
# Requisitos PE2:
# - Kinesis Data Streams PROVISIONED >= 2 shards (30%)
# - Data Firehose -> S3 con buffer 5 MB / 60 s (25%)
# - KMS cifrado del stream y bucket (20%)
# - CloudWatch alarms de monitoreo (15%)
#
# IAM base (roles/trust) vive en el módulo identity (PE1).
# Este módulo agrega policies de KMS y CloudWatch Logs al
# rol de Firehose existente.
#
# BLOQUEADO_POR_COSTO: No ejecutar apply sin aprobación.
# ============================================================

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  account_id  = data.aws_caller_identity.current.account_id
  region      = data.aws_region.current.name
  stream_name = "${var.project_name}-events-${var.environment}"
  bucket_name = "${var.project_name}-bronze-${var.environment}"
}

# ============================================================
# 1. KMS — Customer Managed Key para cifrado
# ============================================================
# Una CMK compartida para Kinesis y S3 bronze.
# Rotación automática habilitada.
# ============================================================

resource "aws_kms_key" "streaming" {
  description             = "CMK para cifrado de Kinesis y S3 bronze - ${var.project_name}"
  deletion_window_in_days = var.kms_deletion_window
  enable_key_rotation     = true

  tags = merge(var.tags, {
    Name = "${var.project_name}-streaming-key-${var.environment}"
  })
}

resource "aws_kms_alias" "streaming" {
  name          = "alias/${var.project_name}-streaming-${var.environment}"
  target_key_id = aws_kms_key.streaming.key_id
}

# ============================================================
# 2. KINESIS DATA STREAM — PROVISIONED
# ============================================================
# Modo PROVISIONED con >= 2 shards (requisito PE2 30%).
# Cifrado con CMK propia.
# Retención configurable (default 24h).
# ============================================================

resource "aws_kinesis_stream" "events" {
  name             = local.stream_name
  shard_count      = var.kinesis_shard_count
  retention_period = var.kinesis_retention_hours

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }

  encryption_type = "KMS"
  kms_key_id      = aws_kms_key.streaming.id

  tags = merge(var.tags, {
    Name = local.stream_name
  })
}

# ============================================================
# 3. S3 BUCKET — Bronze / Raw Layer
# ============================================================
# Destino de Firehose. Cifrado con la misma CMK.
# Versionado, bloqueo de acceso público.
# ============================================================

resource "aws_s3_bucket" "bronze" {
  bucket = local.bucket_name

  tags = merge(var.tags, {
    Name = local.bucket_name
    Tier = "bronze"
  })
}

resource "aws_s3_bucket_versioning" "bronze" {
  bucket = aws_s3_bucket.bronze.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "bronze" {
  bucket = aws_s3_bucket.bronze.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.streaming.arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "bronze" {
  bucket = aws_s3_bucket.bronze.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ============================================================
# 4. CLOUDWATCH — Log Group para Firehose
# ============================================================

resource "aws_cloudwatch_log_group" "firehose" {
  name              = "/aws/firehose/${var.project_name}-bronze-${var.environment}"
  retention_in_days = var.log_retention_days

  tags = merge(var.tags, {
    Name = "${var.project_name}-firehose-logs-${var.environment}"
  })
}

resource "aws_cloudwatch_log_stream" "firehose_s3" {
  name           = "S3Delivery"
  log_group_name = aws_cloudwatch_log_group.firehose.name
}

# ============================================================
# 5. IAM — Policies adicionales para Firehose
# ============================================================
# El rol base (trust + S3 + Kinesis read) vive en identity.
# Aquí agregamos KMS y CloudWatch Logs al mismo rol.
# ============================================================

resource "aws_iam_role_policy" "firehose_kms" {
  name = "kms-streaming-access"
  role = var.firehose_role_name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid    = "KMSForStreamingEncryption"
      Effect = "Allow"
      Action = [
        "kms:Decrypt",
        "kms:GenerateDataKey"
      ]
      Resource = aws_kms_key.streaming.arn
    }]
  })
}

resource "aws_iam_role_policy" "firehose_cloudwatch" {
  name = "cloudwatch-logs-delivery"
  role = var.firehose_role_name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid    = "CloudWatchLogsForFirehose"
      Effect = "Allow"
      Action = [
        "logs:PutLogEvents"
      ]
      Resource = "${aws_cloudwatch_log_group.firehose.arn}:*"
    }]
  })
}

# ============================================================
# 6. FIREHOSE DELIVERY STREAM — Kinesis -> S3 Bronze
# ============================================================
# Buffer: 5 MB / 60 segundos (requisito PE2 25%).
# Prefijo Hive-style para particionamiento temporal.
# Source: Kinesis Data Stream (enhanced fan-out consumer).
# ============================================================

resource "aws_kinesis_firehose_delivery_stream" "s3_bronze" {
  name        = "${var.project_name}-bronze-delivery-${var.environment}"
  destination = "extended_s3"

  kinesis_source_configuration {
    kinesis_stream_arn = aws_kinesis_stream.events.arn
    role_arn           = var.firehose_role_arn
  }

  extended_s3_configuration {
    role_arn            = var.firehose_role_arn
    bucket_arn          = aws_s3_bucket.bronze.arn
    prefix              = "raw/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/"
    error_output_prefix = "errors/!{firehose:error-output-type}/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/"

    buffering_size     = var.firehose_buffer_size
    buffering_interval = var.firehose_buffer_interval

    cloudwatch_logging_options {
      enabled         = true
      log_group_name  = aws_cloudwatch_log_group.firehose.name
      log_stream_name = aws_cloudwatch_log_stream.firehose_s3.name
    }
  }

  tags = merge(var.tags, {
    Name = "${var.project_name}-bronze-delivery-${var.environment}"
  })
}

# ============================================================
# 7. CLOUDWATCH ALARMS — Monitoreo Kinesis
# ============================================================
# Dos alarmas esenciales:
# - IteratorAge: detecta lag de consumidores
# - WriteThrottle: detecta throttling de productores
# ============================================================

resource "aws_cloudwatch_metric_alarm" "iterator_age" {
  alarm_name          = "${var.project_name}-kinesis-iterator-age-${var.environment}"
  alarm_description   = "Alerta cuando el lag del iterador Kinesis supera el umbral"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "GetRecords.IteratorAgeMilliseconds"
  namespace           = "AWS/Kinesis"
  period              = 300
  statistic           = "Maximum"
  threshold           = 60000
  treat_missing_data  = "notBreaching"

  dimensions = {
    StreamName = aws_kinesis_stream.events.name
  }

  tags = merge(var.tags, {
    Name = "${var.project_name}-iterator-age-alarm-${var.environment}"
  })
}

resource "aws_cloudwatch_metric_alarm" "write_throttle" {
  alarm_name          = "${var.project_name}-kinesis-write-throttle-${var.environment}"
  alarm_description   = "Alerta cuando se detecta throttling en escritura al stream"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "WriteProvisionedThroughputExceeded"
  namespace           = "AWS/Kinesis"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  treat_missing_data  = "notBreaching"

  dimensions = {
    StreamName = aws_kinesis_stream.events.name
  }

  tags = merge(var.tags, {
    Name = "${var.project_name}-write-throttle-alarm-${var.environment}"
  })
}
