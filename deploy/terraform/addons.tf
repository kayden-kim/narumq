data "tls_certificate" "eks_oidc" {
  url = aws_eks_cluster.main.identity[0].oidc[0].issuer
}

resource "aws_iam_openid_connect_provider" "eks" {
  url             = aws_eks_cluster.main.identity[0].oidc[0].issuer
  client_id_list  = ["sts.${data.aws_partition.current.dns_suffix}"]
  thumbprint_list = [data.tls_certificate.eks_oidc.certificates[0].sha1_fingerprint]
  tags            = local.common_tags
}

locals {
  oidc_issuer_hostpath = replace(aws_eks_cluster.main.identity[0].oidc[0].issuer, "https://", "")
}

resource "aws_iam_role" "ebs_csi" {
  name = "${var.cluster_name}-ebs-csi"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Federated = aws_iam_openid_connect_provider.eks.arn
      }
      Action = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "${local.oidc_issuer_hostpath}:aud" = "sts.amazonaws.com"
          "${local.oidc_issuer_hostpath}:sub" = "system:serviceaccount:kube-system:ebs-csi-controller-sa"
        }
      }
    }]
  })
  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "ebs_csi" {
  role       = aws_iam_role.ebs_csi.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy"
}

resource "aws_eks_addon" "ebs_csi" {
  cluster_name                = aws_eks_cluster.main.name
  addon_name                  = "aws-ebs-csi-driver"
  service_account_role_arn    = aws_iam_role.ebs_csi.arn
  resolve_conflicts_on_create = "OVERWRITE"
  resolve_conflicts_on_update = "PRESERVE"

  depends_on = [
    aws_iam_role_policy_attachment.ebs_csi,
    aws_eks_node_group.main
  ]

  tags = local.common_tags
}

resource "aws_iam_policy" "load_balancer_controller" {
  name        = "${var.cluster_name}-aws-load-balancer-controller"
  description = "AWS Load Balancer Controller IAM policy (v2.7.2 baseline)."
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "iam:CreateServiceLinkedRole"
        ]
        Resource = "*"
        Condition = {
          StringEquals = { "iam:AWSServiceName" = "elasticloadbalancing.amazonaws.com" }
        }
      },
      {
        Effect = "Allow"
        Action = [
          "ec2:Describe*", "ec2:GetCoipPoolUsage", "ec2:GetSecurityGroupsForVpc",
          "elasticloadbalancing:Describe*"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "ec2:AuthorizeSecurityGroupIngress", "ec2:RevokeSecurityGroupIngress",
          "ec2:CreateSecurityGroup", "ec2:DeleteSecurityGroup",
          "ec2:CreateTags", "ec2:DeleteTags"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "elasticloadbalancing:CreateLoadBalancer", "elasticloadbalancing:CreateTargetGroup",
          "elasticloadbalancing:CreateListener", "elasticloadbalancing:CreateRule",
          "elasticloadbalancing:DeleteLoadBalancer", "elasticloadbalancing:DeleteTargetGroup",
          "elasticloadbalancing:DeleteListener", "elasticloadbalancing:DeleteRule",
          "elasticloadbalancing:ModifyLoadBalancerAttributes", "elasticloadbalancing:SetIpAddressType",
          "elasticloadbalancing:SetSecurityGroups", "elasticloadbalancing:SetSubnets",
          "elasticloadbalancing:ModifyTargetGroup", "elasticloadbalancing:ModifyTargetGroupAttributes",
          "elasticloadbalancing:ModifyListener", "elasticloadbalancing:ModifyRule",
          "elasticloadbalancing:AddTags", "elasticloadbalancing:RemoveTags",
          "elasticloadbalancing:RegisterTargets", "elasticloadbalancing:DeregisterTargets"
        ]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["waf-regional:GetWebACLForResource", "waf-regional:GetWebACL", "waf:GetWebACL", "wafv2:GetWebACL", "wafv2:GetWebACLForResource", "shield:GetSubscriptionState", "shield:DescribeProtection", "shield:CreateProtection", "shield:DeleteProtection"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["ec2:ModifySecurityGroupRules"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["elasticloadbalancing:AddListenerCertificates", "elasticloadbalancing:RemoveListenerCertificates", "elasticloadbalancing:ModifyCapacityReservation"]
        Resource = "*"
      }
    ]
  })
  tags = local.common_tags
}

resource "aws_iam_role" "load_balancer_controller" {
  name = "${var.cluster_name}-aws-load-balancer-controller"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Federated = aws_iam_openid_connect_provider.eks.arn }
      Action    = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "${local.oidc_issuer_hostpath}:aud" = "sts.amazonaws.com"
          "${local.oidc_issuer_hostpath}:sub" = "system:serviceaccount:kube-system:aws-load-balancer-controller"
        }
      }
    }]
  })
  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "load_balancer_controller" {
  role       = aws_iam_role.load_balancer_controller.name
  policy_arn = aws_iam_policy.load_balancer_controller.arn
}

resource "kubernetes_storage_class_v1" "gp3" {
  metadata {
    name = "gp3"
    annotations = {
      "storageclass.kubernetes.io/is-default-class" = "true"
    }
  }

  storage_provisioner    = "ebs.csi.aws.com"
  reclaim_policy         = "Delete"
  volume_binding_mode    = "WaitForFirstConsumer"
  allow_volume_expansion = true

  parameters = {
    type      = "gp3"
    encrypted = "true"
  }

  depends_on = [aws_eks_addon.ebs_csi]
}
