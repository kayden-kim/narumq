variable "aws_region" {
  description = "AWS region for the EKS deployment."
  type        = string
  default     = "ap-northeast-2"
}

variable "cluster_name" {
  description = "Name of the EKS cluster."
  type        = string
  default     = "narumq-eks"
}

variable "kubernetes_version" {
  description = "EKS Kubernetes minor version supported in the selected region."
  type        = string
  default     = "1.37"
}

variable "vpc_cidr" {
  description = "CIDR block for the dedicated EKS VPC."
  type        = string
  default     = "10.0.0.0/16"
}

variable "availability_zones" {
  description = "Exactly three Availability Zones for the worker and subnet topology."
  type        = list(string)
  default     = ["ap-northeast-2a", "ap-northeast-2b", "ap-northeast-2c"]

  validation {
    condition     = length(var.availability_zones) == 3 && length(distinct(var.availability_zones)) == 3
    error_message = "availability_zones must contain exactly three unique zones."
  }
}

variable "single_nat_gateway" {
  description = "Use one NAT Gateway for all private subnets to reduce sandbox cost."
  type        = bool
  default     = true
}

variable "cluster_endpoint_public_access_cidrs" {
  description = "IPv4 CIDRs allowed to reach the public Kubernetes API endpoint. Leave empty for private-only access."
  type        = list(string)
  default     = []

  validation {
    condition     = !contains(var.cluster_endpoint_public_access_cidrs, "0.0.0.0/0")
    error_message = "cluster_endpoint_public_access_cidrs must not contain 0.0.0.0/0. Specify your restricted operator CIDR (e.g., your-ip/32)."
  }
}

variable "node_instance_types" {
  description = "ARM64 EC2 instance types for the managed node group."
  type        = list(string)
  default     = ["m7g.large"]
}

variable "node_desired_size" {
  description = "Desired managed node group size."
  type        = number
  default     = 3
}

variable "node_min_size" {
  description = "Minimum managed node group size."
  type        = number
  default     = 3
}

variable "node_max_size" {
  description = "Maximum managed node group size."
  type        = number
  default     = 6
}

variable "tags" {
  description = "Additional tags applied to provisioned AWS resources."
  type        = map(string)
  default     = {}
}
