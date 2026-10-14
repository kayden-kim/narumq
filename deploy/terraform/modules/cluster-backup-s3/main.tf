locals {
  role_prefix = substr(var.cluster_name, 0, 44)
  bucket_arn  = "arn:${data.aws_partition.current.partition}:s3:::${var.bucket_name}"
  object_arn  = "${local.bucket_arn}/${var.prefix}/*"
}

data "aws_partition" "current" {}

resource "aws_s3_bucket" "backup" {
  bucket        = var.bucket_name
  force_destroy = var.force_destroy
  tags          = var.tags
}

resource "aws_s3_bucket_public_access_block" "backup" {
  bucket                  = aws_s3_bucket.backup.id
  block_public_acls       = true
  ignore_public_acls      = true
  block_public_policy     = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "backup" {
  bucket = aws_s3_bucket.backup.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "backup" {
  bucket = aws_s3_bucket.backup.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "backup" {
  bucket = aws_s3_bucket.backup.id

  rule {
    id     = "expire-backups-after-30-days"
    status = "Enabled"

    filter {
      prefix = "${var.prefix}/"
    }

    expiration {
      days = 30
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

resource "aws_s3_bucket_policy" "backup" {
  bucket = aws_s3_bucket.backup.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "DenyInsecureTransport"
        Effect    = "Deny"
        Principal = "*"
        Action    = "s3:*"
        Resource  = [local.bucket_arn, "${local.bucket_arn}/*"]
        Condition = {
          Bool = { "aws:SecureTransport" = "false" }
        }
      },
      {
        Sid       = "DenyNonSSECustomerManagedOrUnencryptedUploads"
        Effect    = "Deny"
        Principal = "*"
        Action    = "s3:PutObject"
        Resource  = "${local.bucket_arn}/*"
        Condition = {
          StringNotEquals = { "s3:x-amz-server-side-encryption" = "AES256" }
        }
      }
    ]
  })

  depends_on = [aws_s3_bucket_public_access_block.backup]
}

resource "aws_iam_role" "writer" {
  name = "${local.role_prefix}-backup-writer"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = { Federated = var.oidc_provider_arn }
      Action = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "${var.oidc_issuer_hostpath}:aud" = "sts.amazonaws.com"
          "${var.oidc_issuer_hostpath}:sub" = "system:serviceaccount:${var.namespace}:narumq-backup"
        }
      }
    }]
  })
  tags = var.tags
}

resource "aws_iam_role_policy" "writer" {
  name = "${local.role_prefix}-backup-upload-only"
  role = aws_iam_role.writer.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action   = ["s3:PutObject"]
      Resource = local.object_arn
    }]
  })
}

resource "aws_iam_role" "restore" {
  name = "${local.role_prefix}-backup-restore"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = { Federated = var.oidc_provider_arn }
      Action = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "${var.oidc_issuer_hostpath}:aud" = "sts.amazonaws.com"
          "${var.oidc_issuer_hostpath}:sub" = "system:serviceaccount:${var.namespace}:narumq-backup-restore"
        }
      }
    }]
  })
  tags = var.tags
}

resource "aws_iam_role_policy" "restore" {
  name = "${local.role_prefix}-backup-read-only"
  role = aws_iam_role.restore.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = local.bucket_arn
        Condition = {
          StringLike = { "s3:prefix" = ["${var.prefix}/", "${var.prefix}/*"] }
        }
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = local.object_arn
      }
    ]
  })
}

resource "kubernetes_service_account_v1" "writer" {
  metadata {
    name      = "narumq-backup"
    namespace = var.namespace
    annotations = {
      "eks.amazonaws.com/role-arn" = aws_iam_role.writer.arn
    }
  }
  automount_service_account_token = true
}

resource "kubernetes_service_account_v1" "restore" {
  metadata {
    name      = "narumq-backup-restore"
    namespace = var.namespace
    annotations = {
      "eks.amazonaws.com/role-arn" = aws_iam_role.restore.arn
    }
  }
  automount_service_account_token = true
}

resource "kubernetes_config_map_v1" "settings" {
  metadata {
    name      = "narumq-backup-config"
    namespace = var.namespace
  }
  data = {
    BACKUP_S3_BUCKET = var.bucket_name
    BACKUP_S3_PREFIX = var.prefix
    AWS_REGION       = var.region
  }
}
