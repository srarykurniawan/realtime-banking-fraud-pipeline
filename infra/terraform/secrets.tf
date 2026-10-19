# Snowflake key-pair credentials. Values are set out-of-band (never in Terraform state):
#   aws secretsmanager put-secret-value --secret-id fraud-prod/snowflake/stream \
#     --secret-string '{"account":"...","user":"SVC_FRAUD_STREAM","private_key":"-----BEGIN PRIVATE KEY-----..."}'
resource "aws_secretsmanager_secret" "snowflake_stream" {
  name        = "${local.name}/snowflake/stream"
  description = "Snowflake key-pair for SVC_FRAUD_STREAM (Spark writer)"
  kms_key_id  = aws_kms_key.fraud.arn
}

resource "aws_secretsmanager_secret" "snowflake_airflow" {
  name        = "${local.name}/snowflake/airflow"
  description = "Snowflake key-pair for SVC_FRAUD_AIRFLOW (orchestrator)"
  kms_key_id  = aws_kms_key.fraud.arn
}
