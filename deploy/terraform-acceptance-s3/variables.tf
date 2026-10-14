variable "aws_region" {
  description = "AWS region containing the disposable acceptance cluster and backup bucket."
  type        = string
  default     = "ap-northeast-2"
}

variable "fixture_cluster_name" {
  description = "Name of the disposable CLU-14 acceptance EKS cluster; production narumq-eks is rejected."
  type        = string
  nullable    = false

  validation {
    condition     = startswith(var.fixture_cluster_name, "narumq-ha-acceptance-") && var.fixture_cluster_name != "narumq-eks"
    error_message = "fixture_cluster_name must identify the disposable narumq-ha-acceptance-* cluster, never narumq-eks."
  }
}

variable "backup_bucket_name" {
  description = "Unique name for the disposable fixture's private S3 backup bucket."
  type        = string
  nullable    = false
}

variable "backup_prefix" {
  description = "Exact fixture-cluster prefix within the backup bucket."
  type        = string
  nullable    = false
}
