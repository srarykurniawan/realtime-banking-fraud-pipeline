variable "region" {
  type    = string
  default = "ap-southeast-3" # Jakarta
}

variable "environment" {
  type    = string
  default = "prod"
}

variable "msk_cluster_arn" {
  type        = string
  description = "ARN of the existing MSK cluster carrying txn.raw.* topics"
}

variable "eks_oidc_provider_arn" {
  type        = string
  description = "OIDC provider of the EKS cluster running the Spark job (IRSA)"
}

variable "eks_oidc_provider_url" {
  type        = string
  description = "OIDC issuer URL without https:// (e.g. oidc.eks.ap-southeast-3.amazonaws.com/id/XXXX)"
}

variable "spark_namespace" {
  type    = string
  default = "fraud"
}

variable "spark_service_account" {
  type    = string
  default = "fraud-stream"
}

variable "mwaa_execution_role_name" {
  type        = string
  description = "Existing MWAA execution role that runs the fraud DAGs"
}

variable "alert_emails" {
  type    = list(string)
  default = ["fraud-oncall@bank.co.id"]
}

variable "bronze_retention_days" {
  type    = number
  default = 3650
}

variable "max_quarantine_ratio" {
  type    = number
  default = 0.05
}

variable "max_event_lag_seconds" {
  type    = number
  default = 180
}
