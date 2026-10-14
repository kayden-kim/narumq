output "bucket_name" {
  description = "Private S3 bucket name used for cluster backup archives."
  value       = aws_s3_bucket.backup.bucket
}

output "prefix" {
  description = "Exact S3 key prefix containing this cluster's backup archives."
  value       = var.prefix
}

output "writer_role_arn" {
  description = "IRSA role ARN for the upload-only narumq-backup service account."
  value       = aws_iam_role.writer.arn
}

output "restore_role_arn" {
  description = "IRSA role ARN for the prefix-scoped read-only narumq-backup-restore service account."
  value       = aws_iam_role.restore.arn
}
