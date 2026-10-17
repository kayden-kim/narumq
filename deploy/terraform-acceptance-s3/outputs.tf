output "backup_bucket_name" {
  description = "Private S3 bucket created for the disposable backup acceptance fixture."
  value       = module.cluster_backup_s3.bucket_name
}

output "backup_prefix" {
  description = "Exact cluster prefix covered by writer and restore roles."
  value       = module.cluster_backup_s3.prefix
}

output "backup_writer_role_arn" {
  description = "Upload-only IRSA role for the fixture's narumq-backup service account."
  value       = module.cluster_backup_s3.writer_role_arn
}

output "backup_restore_role_arn" {
  description = "Read-only IRSA role for the fixture's narumq-backup-restore service account."
  value       = module.cluster_backup_s3.restore_role_arn
}
