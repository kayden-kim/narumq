output "cluster_name" {
  description = "EKS cluster name."
  value       = aws_eks_cluster.main.name
}

output "cluster_endpoint" {
  description = "Kubernetes API endpoint for the EKS cluster."
  value       = aws_eks_cluster.main.endpoint
}

output "cluster_certificate_authority_data" {
  description = "Base64-encoded cluster certificate authority data."
  value       = aws_eks_cluster.main.certificate_authority[0].data
}

output "vpc_id" {
  description = "ID of the dedicated EKS VPC."
  value       = aws_vpc.main.id
}

output "configure_kubectl_command" {
  description = "AWS CLI command to add this cluster to the current kubeconfig."
  value       = "aws eks update-kubeconfig --region ${var.aws_region} --name ${var.cluster_name} --profile kayden-sandbox-admin"
}

output "load_balancer_controller_role_arn" {
  description = "IRSA role ARN to configure on the aws-load-balancer-controller service account."
  value       = aws_iam_role.load_balancer_controller.arn
}
output "backup_s3_bucket_name" {
  description = "Private cluster backup bucket name, or null when S3 backups are disabled."
  value       = var.backup_s3_enabled ? module.cluster_backup_s3[0].bucket_name : null
}

output "backup_s3_prefix" {
  description = "Cluster backup key prefix, or null when S3 backups are disabled."
  value       = var.backup_s3_enabled ? module.cluster_backup_s3[0].prefix : null
}

output "backup_s3_writer_role_arn" {
  description = "Upload-only backup IRSA role ARN, or null when S3 backups are disabled."
  value       = var.backup_s3_enabled ? module.cluster_backup_s3[0].writer_role_arn : null
}

output "backup_s3_restore_role_arn" {
  description = "Prefix-scoped read-only backup IRSA role ARN, or null when S3 backups are disabled."
  value       = var.backup_s3_enabled ? module.cluster_backup_s3[0].restore_role_arn : null
}
