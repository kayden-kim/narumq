provider "aws" {
  region = var.aws_region
}

data "aws_eks_cluster" "fixture" {
  name = var.fixture_cluster_name
}

data "aws_eks_cluster_auth" "fixture" {
  name = data.aws_eks_cluster.fixture.name
}

data "aws_iam_openid_connect_provider" "fixture" {
  url = data.aws_eks_cluster.fixture.identity[0].oidc[0].issuer
}

provider "kubernetes" {
  host                   = data.aws_eks_cluster.fixture.endpoint
  cluster_ca_certificate = base64decode(data.aws_eks_cluster.fixture.certificate_authority[0].data)
  token                  = data.aws_eks_cluster_auth.fixture.token
}

locals {
  oidc_issuer_hostpath = replace(data.aws_eks_cluster.fixture.identity[0].oidc[0].issuer, "https://", "")
  common_tags = {
    Application = "NaruMQ"
    Environment = "acceptance"
    ManagedBy   = "Terraform"
    Purpose     = "temporary-cluster-backup-acceptance"
  }
}

module "cluster_backup_s3" {
  source = "../terraform/modules/cluster-backup-s3"

  cluster_name         = var.fixture_cluster_name
  bucket_name          = var.backup_bucket_name
  prefix               = var.backup_prefix
  force_destroy        = true
  region               = var.aws_region
  namespace            = "narumq"
  oidc_provider_arn    = data.aws_iam_openid_connect_provider.fixture.arn
  oidc_issuer_hostpath = local.oidc_issuer_hostpath
  tags                 = local.common_tags
}
