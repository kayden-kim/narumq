variable "cluster_name" {
  description = "EKS cluster name used to scope IRSA role names."
  type        = string
}

variable "bucket_name" {
  description = "Unique name for the private backup bucket in the EKS region."
  type        = string
  nullable    = false

  validation {
    condition = (
      length(var.bucket_name) >= 3 && length(var.bucket_name) <= 63 &&
      can(regex("^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$", var.bucket_name)) &&
      !strcontains(var.bucket_name, "..") &&
      !strcontains(var.bucket_name, ".-") &&
      !strcontains(var.bucket_name, "-.")
    )
    error_message = "bucket_name must be a valid lowercase S3 bucket name."
  }
}

variable "force_destroy" {
  description = "Whether Terraform may empty this bucket during destroy; enable only for disposable acceptance fixtures."
  type        = bool
  default     = false
}

variable "prefix" {
  description = "Exact cluster backup prefix within the bucket, without a leading or trailing slash."
  type        = string
  nullable    = false

  validation {
    condition = (
      length(var.prefix) > 0 &&
      can(regex("^[A-Za-z0-9][A-Za-z0-9._/-]*$", var.prefix)) &&
      !strcontains(var.prefix, "..") &&
      !strcontains(var.prefix, "//") &&
      !endswith(var.prefix, "/")
    )
    error_message = "prefix must be a non-empty relative S3 key prefix without empty or parent path segments."
  }
}

variable "region" {
  description = "Region shared by the S3 bucket and EKS cluster."
  type        = string
}

variable "namespace" {
  description = "Existing Kubernetes namespace containing backup workloads."
  type        = string
  default     = "narumq"
}

variable "oidc_provider_arn" {
  description = "ARN of this EKS cluster's IAM OIDC provider."
  type        = string
}

variable "oidc_issuer_hostpath" {
  description = "EKS OIDC issuer host/path without the https:// prefix."
  type        = string
}

variable "tags" {
  description = "AWS resource tags."
  type        = map(string)
  default     = {}
}
