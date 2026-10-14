variable "cluster_endpoint_public_access_cidrs" {
  description = "Restricted operator CIDRs for the disposable fixture Kubernetes API."
  type        = list(string)

  validation {
    condition     = length(var.cluster_endpoint_public_access_cidrs) > 0 && alltrue([for cidr in var.cluster_endpoint_public_access_cidrs : cidr != "0.0.0.0/0"])
    error_message = "Set at least one restricted operator CIDR; unrestricted API access is forbidden."
  }
}

module "fixture" {
  source = "../terraform"

  aws_region                          = "ap-northeast-2"
  cluster_name                        = "narumq-ha-acceptance-20261014"
  kubernetes_version                  = "1.37"
  vpc_cidr                            = "10.37.0.0/16"
  availability_zones                  = ["ap-northeast-2a", "ap-northeast-2b", "ap-northeast-2c"]
  single_nat_gateway                  = true
  cluster_endpoint_public_access_cidrs = var.cluster_endpoint_public_access_cidrs
  node_instance_types                 = ["m7g.large"]
  node_desired_size                   = 4
  node_min_size                       = 3
  node_max_size                       = 4
  tags = {
    Purpose   = "disposable-acceptance-fixture"
    Workstream = "CLU-14-SPEC-TBD-09-DEP-13-23"
    DataClass = "synthetic-test-data-only"
  }
}
