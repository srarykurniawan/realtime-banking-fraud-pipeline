# =====================================================================
# Least-privilege IAM. Two workload identities:
#   * fraud-stream (Spark on EKS via IRSA): read txn.raw.*, write alerts/DLQ topics,
#     read/write its own S3 prefixes, read ONE secret, put metrics in ONE namespace.
#   * Airflow (MWAA): read the warehouse secret, publish to the alert SNS topic.
# No wildcard resources except where the AWS API does not support resource scoping
# (cloudwatch:PutMetricData), which is then restricted by condition.
# =====================================================================
locals {
  msk_parts      = split("/", var.msk_cluster_arn)
  msk_cluster    = "${local.msk_parts[1]}/${local.msk_parts[2]}"
  msk_arn_prefix = "arn:aws:kafka:${var.region}:${local.account_id}"
}

data "aws_iam_policy_document" "stream_assume" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = [var.eks_oidc_provider_arn]
    }
    condition {
      test     = "StringEquals"
      variable = "${var.eks_oidc_provider_url}:sub"
      values   = ["system:serviceaccount:${var.spark_namespace}:${var.spark_service_account}"]
    }
    condition {
      test     = "StringEquals"
      variable = "${var.eks_oidc_provider_url}:aud"
      values   = ["sts.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "stream" {
  name                 = "${local.name}-stream"
  assume_role_policy   = data.aws_iam_policy_document.stream_assume.json
  max_session_duration = 3600
}

data "aws_iam_policy_document" "stream" {
  statement {
    sid       = "MskConnect"
    actions   = ["kafka-cluster:Connect", "kafka-cluster:DescribeCluster"]
    resources = [var.msk_cluster_arn]
  }
  statement {
    sid       = "MskReadRawTopics"
    actions   = ["kafka-cluster:ReadData", "kafka-cluster:DescribeTopic"]
    resources = ["${local.msk_arn_prefix}:topic/${local.msk_cluster}/txn.raw.*"]
  }
  statement {
    sid     = "MskWriteAlertAndDlq"
    actions = ["kafka-cluster:WriteData", "kafka-cluster:DescribeTopic"]
    resources = [
      "${local.msk_arn_prefix}:topic/${local.msk_cluster}/fraud.alerts",
      "${local.msk_arn_prefix}:topic/${local.msk_cluster}/txn.dlq",
    ]
  }
  statement {
    sid       = "MskConsumerGroup"
    actions   = ["kafka-cluster:AlterGroup", "kafka-cluster:DescribeGroup"]
    resources = ["${local.msk_arn_prefix}:group/${local.msk_cluster}/spark-kafka-source-*"]
  }
  statement {
    sid     = "LakeReadWrite"
    actions = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = [
      "${aws_s3_bucket.lake.arn}/bronze/*",
      "${aws_s3_bucket.lake.arn}/checkpoints/*",
    ]
  }
  statement {
    sid       = "LakeList"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.lake.arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["bronze/*", "checkpoints/*"]
    }
  }
  statement {
    sid       = "KmsForLakeAndSecret"
    actions   = ["kms:Decrypt", "kms:GenerateDataKey"]
    resources = [aws_kms_key.fraud.arn]
  }
  statement {
    sid       = "ReadWarehouseSecret"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [aws_secretsmanager_secret.snowflake_stream.arn]
  }
  statement {
    sid       = "PutPipelineMetrics"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"] # API has no resource-level scoping...
    condition {       # ...so restrict to our namespace
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = ["FraudPlatform"]
    }
  }
}

resource "aws_iam_role_policy" "stream" {
  name   = "fraud-stream-least-privilege"
  role   = aws_iam_role.stream.id
  policy = data.aws_iam_policy_document.stream.json
}

# ------------------------------------------------------------------ Airflow (MWAA)
data "aws_iam_policy_document" "airflow" {
  statement {
    sid       = "ReadWarehouseSecret"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [aws_secretsmanager_secret.snowflake_airflow.arn]
  }
  statement {
    sid       = "Kms"
    actions   = ["kms:Decrypt", "kms:GenerateDataKey"]
    resources = [aws_kms_key.fraud.arn]
  }
  statement {
    sid       = "PublishAlerts"
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.alerts.arn]
  }
}

resource "aws_iam_role_policy" "airflow" {
  name   = "fraud-airflow"
  role   = var.mwaa_execution_role_name
  policy = data.aws_iam_policy_document.airflow.json
}
