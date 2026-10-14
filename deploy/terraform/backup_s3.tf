module "cluster_backup_s3" {
  count  = var.backup_s3_enabled ? 1 : 0
  source = "./modules/cluster-backup-s3"

  cluster_name         = var.cluster_name
  bucket_name          = var.backup_s3_bucket_name
  prefix               = var.backup_s3_prefix
  region               = var.aws_region
  oidc_provider_arn    = aws_iam_openid_connect_provider.eks.arn
  oidc_issuer_hostpath = local.oidc_issuer_hostpath
  tags                 = local.common_tags
}
