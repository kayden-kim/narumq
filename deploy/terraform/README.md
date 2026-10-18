# EKS infrastructure

This Terraform root provisions the NaruMQ EKS EC2 qualification profile in `ap-northeast-2`: a three-AZ VPC, public and private subnets tagged for Kubernetes load-balancer discovery, NAT egress, an EKS control plane, ARM64 managed nodes, core EKS add-ons, IRSA roles, and an encrypted default `gp3` StorageClass.

## Prerequisites

- Terraform `>= 1.5.0`
- AWS CLI credentials for account `809446814769` using profile `kayden-sandbox-admin`
- Permissions to create VPC, EC2, EKS, IAM, CloudWatch Logs, and Kubernetes resources
- Network access to AWS APIs and the EKS cluster endpoint when applying the StorageClass
- The Kubernetes API endpoint is private-only by default. Set `cluster_endpoint_public_access_cidrs` to restricted operator IPv4 CIDRs to enable public access; `0.0.0.0/0` is rejected.

The cluster's default Kubernetes version is `1.37`. This is the current supported version confirmed for `ap-northeast-2` using `aws eks describe-addon-versions --kubernetes-version 1.37`; select another EKS-supported version using `-var kubernetes_version=...` if required. EKS add-on versions are selected by AWS when unspecified.

The CoreDNS add-on runs two replicas with required hostname anti-affinity and a zone topology spread (`DoNotSchedule`), so one worker loss cannot remove every cluster DNS endpoint. This reduces, but does not remove, short `unknown` node reports after a worker loss; see [EKS architecture](../../docs/architectures/eks-ec2-deployment.md#placement와-availability-constraint).

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
| `backup_s3_enabled` | `false` | Create the private S3 backup bucket, upload-only writer and prefix-scoped restore IRSA roles, service accounts, and backup settings |
| `backup_s3_bucket_name` | `null` | Unique S3 bucket name; required when `backup_s3_enabled=true` |
| `backup_s3_prefix` | `null` | Exact relative key prefix for this cluster; required when `backup_s3_enabled=true` |
| `tags` | `{}` | Extra tags for provisioned AWS resources |

Set AWS credentials through the named profile, for example `AWS_PROFILE=kayden-sandbox-admin` in POSIX shells or `$env:AWS_PROFILE = 'kayden-sandbox-admin'` in PowerShell. AWS provider credentials are not embedded in Terraform configuration.

## Optional S3 backup integration

The shared EKS Terraform root leaves S3 backup disabled by default, so `narumq-eks` is unchanged unless `backup_s3_enabled=true` is explicitly selected with a bucket name and cluster prefix. The opt-in module creates the private same-region bucket, lifecycle controls, exact-subject writer and restore IRSA roles, `narumq-backup` and `narumq-backup-restore` service accounts, and the namespace configuration map. The `narumq` namespace must already exist before enabling the module. Apply the backup deployment's opt-in Kustomize overlay only after the module has created its service account and settings.

For disposable acceptance, use the separate `deploy/terraform-acceptance-s3` root and its own local state, with the existing `narumq-ha-acceptance-*` fixture name. It rejects `narumq-eks`; do not use the shared root or production cluster for fixture smoke. Destroy the acceptance root after evidence collection so its S3 bucket, IAM roles, service accounts, and settings are removed.

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

운영 데이터 보존 전에는 삭제하지 않는다. `gp3` StorageClass의 reclaim policy는 `Delete`이므로 PVC 삭제가 EBS 데이터 삭제로 이어질 수 있다. 삭제 승인은 롤아웃·장애·복원 시험 승인이 아니다.

1. AWS account, cluster ARN, VPC와 Terraform state의 lineage·serial 및 실제 관리 대상 ID를 대조한다.
2. 필요한 online backup, voter 설정·인증 자료, node-local 감사 기록과 Terraform state를 비공개 저장소에 보존한다. archive checksum을 검증한다.
3. 현재 state에서 `terraform plan -destroy -out=<private-plan-path>`로 새 계획을 만든다. managed action이 정확히 `delete` 또는 `no-op`인지, 대상이 승인된 전용 자원인지 확인한다. 보호 snapshot과 공유 ECR은 삭제 대상에 넣지 않는다.
4. 보존을 확인한 뒤 인벤토리에 있는 전용 workload와 Service를 삭제한다. AWS Load Balancer Controller와 EBS CSI가 살아 있는 동안 NLB·target group·PVC/EBS 정리를 완료한다. 이는 최종 폐기이며 수용 시험이 아니다.
5. 검토한 계획을 `terraform apply <private-plan-path>`로 실행한다. state가 바뀌면 이전 계획을 폐기하고 다시 만든다.
6. EKS, nodegroup, VPC, NAT/EIP, NLB·target group·전용 security group, EBS, IAM 및 전용 log group을 직접 조회해 잔여 자원을 확인한다. Tagging API의 오래된 기록만으로 존재 여부를 판단하지 않는다.

2026-10-15 사전 보존은 Windows DPAPI `CurrentUser`를 사용했다. 해당 Windows 사용자 프로필에서만 복호화할 수 있다. `C:/Users/kayden.kim/AppData/Local/narumq/preservation/eks-final-20261014T221024Z`의 `.dpapi` 파일과 사용자 프로필을 함께 보호한다. 복호화는 PowerShell의 `[Security.Cryptography.ProtectedData]::Unprotect(bytes, $null, [Security.Cryptography.DataProtectionScope]::CurrentUser)`를 사용한다. 복호화한 Secret·archive·state를 Git이나 로그에 넣지 않는다. 이 로컬 보존은 외부 재해 복구나 30일 S3 보존 수용 증거가 아니다.

단일 NAT Gateway와 AZ 간 NAT egress는 sandbox 비용 절감 설정이다. AZ 장애에 강한 production topology를 입증하지 않는다.

### 2026-10-15 최종 정리 기록

사용자의 기존 `narumq-eks` 삭제 승인에 따라 Terraform state lineage `103d4690-cf1d-a395-505e-eae2e875c471`, serial `44`의 관리 주소·ID 41개를 검토했다. 전용 namespace와 NLB·PVC 정리를 먼저 확인한 뒤 새 saved plan을 적용해 `0 added, 0 changed, 41 destroyed`로 끝났다. 최종 state는 serial `90`, resource 0개다. 전용 CloudWatch log group도 삭제했다. 전체 제어 평면 로그 수집은 필요하지 않아 중단했고 부분 수집 파일을 제거했다. 최종 backup·설정·node-local 감사 기록과 state는 위 DPAPI directory에 보존했다.

보호 snapshot `snap-01999990697d01221`, `snap-0e3a75c648325b3cd`, `snap-059315c3eb5c3bae4`와 공유 `narumq` ECR repository의 `observability-20261014` image는 유지했다. 이 보존 예외를 포함해 AWS 비용이 모두 0이 됐다고 주장하지 않는다. 직접 조회 결과와 미검증 gate는 `.artifacts/verification/eks-final-20261014T221024Z/final-decommission-summary.json`에 있다. 최종 폐기는 rollout·장애·복원 수용 시험을 대신하지 않는다.
