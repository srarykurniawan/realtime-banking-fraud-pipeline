output "lake_bucket" {
  value = aws_s3_bucket.lake.bucket
}

output "stream_role_arn" {
  description = "Annotate the fraud-stream Kubernetes service account with this role (IRSA)"
  value       = aws_iam_role.stream.arn
}

output "alerts_sns_topic_arn" {
  description = "Set as Airflow Variable fraud_sns_topic_arn"
  value       = aws_sns_topic.alerts.arn
}

output "snowflake_stream_secret_arn" {
  value = aws_secretsmanager_secret.snowflake_stream.arn
}

output "kms_key_arn" {
  value = aws_kms_key.fraud.arn
}
