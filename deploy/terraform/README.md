# EKS infrastructure

This Terraform root provisions the NaruMQ EKS EC2 qualification profile in `ap-northeast-2`: a three-AZ VPC, public and private subnets tagged for Kubernetes load-balancer discovery, NAT egress, an EKS control plane, ARM64 managed nodes, core EKS add-ons, IRSA roles, and an encrypted default `gp3` StorageClass.

## Prerequisites

- Terraform `>= 1.5.0`
- AWS CLI credentials for account `809446814769` using profile `kayden-sandbox-admin`
- Permissions to create VPC, EC2, EKS, IAM, CloudWatch Logs, and Kubernetes resources
- Network access to AWS APIs and the EKS cluster endpoint when applying the StorageClass
- The Kubernetes API endpoint is private-only by default. Set `cluster_endpoint_public_access_cidrs` to restricted operator IPv4 CIDRs to enable public access; `0.0.0.0/0` is rejected.

The cluster's default Kubernetes version is `1.37`. This is the current supported version confirmed for `ap-northeast-2` using `aws eks describe-addon-versions --kubernetes-version 1.37`; select another EKS-supported version using `-var kubernetes_version=...` if required. EKS add-on versions are selected by AWS when unspecified.

## Configuration

| Variable | Default | Description |
| --- | --- | --- |
| `aws_region` | `ap-northeast-2` | AWS deployment region |
| `cluster_name` | `narumq-eks` | EKS cluster name |
| `kubernetes_version` | `1.37` | EKS Kubernetes minor version |
| `vpc_cidr` | `10.0.0.0/16` | VPC CIDR |
| `availability_zones` | `ap-northeast-2a`, `ap-northeast-2b`, `ap-northeast-2c` | Three subnet/AZ locations |
| `single_nat_gateway` | `true` | Share one NAT Gateway to reduce sandbox cost; set `false` for one NAT per AZ |
| `cluster_endpoint_public_access_cidrs` | `[]` | Empty disables the public API endpoint; provide restricted operator CIDRs (for example, `YOUR_PUBLIC_IP/32`) to enable it |
| `node_instance_types` | `m7g.large` | ARM64 managed worker instance types |
| `node_desired_size` | `3` | Desired worker count |
| `node_min_size` | `3` | Minimum worker count |
| `node_max_size` | `6` | Maximum worker count |
| `tags` | `{}` | Extra tags for provisioned AWS resources |

Set AWS credentials through the named profile, for example `AWS_PROFILE=kayden-sandbox-admin` in POSIX shells or `$env:AWS_PROFILE = 'kayden-sandbox-admin'` in PowerShell. AWS provider credentials are not embedded in Terraform configuration.

## Provisioning

From this directory:

Copy `terraform.tfvars.example` to `terraform.tfvars` and replace `YOUR_PUBLIC_IP/32` with your trusted operator IP CIDR. The checked-in `terraform.tfvars` is configured with the current operator IP `/32`; update it if the operator's public IP changes.

```sh
terraform init
terraform plan
terraform apply
```

The public endpoint is limited to the CIDRs in `terraform.tfvars`; with an empty CIDR list the API endpoint is private-only. Outputs include the cluster endpoint, certificate authority data, VPC ID, and the exact `aws eks update-kubeconfig` command.

## Teardown

```sh
terraform destroy
```

The `gp3` StorageClass uses `Delete` reclaim policy; persistent volumes created from it may therefore be deleted when their claims are removed. Back up any required data before deleting workloads or destroying this infrastructure. A single NAT Gateway and NAT egress across AZs are a sandbox cost optimization, not an AZ-resilient production topology.
