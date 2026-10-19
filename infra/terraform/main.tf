terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.60"
    }
  }
  # backend "s3" {
  #   bucket         = "bank-terraform-state"
  #   key            = "fraud-platform/terraform.tfstate"
  #   region         = "ap-southeast-3"
  #   dynamodb_table = "terraform-locks"
  #   encrypt        = true
  # }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = {
      Project            = "banking-fraud-platform"
      Environment        = var.environment
      Owner              = "fraud-data-platform"
      DataClassification = "confidential"
    }
  }
}

data "aws_caller_identity" "current" {}

locals {
  name       = "fraud-${var.environment}"
  account_id = data.aws_caller_identity.current.account_id
}

# ------------------------------------------------------------------ encryption
resource "aws_kms_key" "fraud" {
  description             = "${local.name} data encryption (S3 bronze, checkpoints, secrets, SNS)"
  enable_key_rotation     = true
  deletion_window_in_days = 30
}

resource "aws_kms_alias" "fraud" {
  name          = "alias/${local.name}"
  target_key_id = aws_kms_key.fraud.key_id
}

# ------------------------------------------------------------------ storage (bronze + checkpoints)
resource "aws_s3_bucket" "lake" {
  bucket = "${local.name}-lake-${local.account_id}"
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.fraud.arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    id     = "bronze-tiering"
    status = "Enabled"
    filter {
      prefix = "bronze/"
    }
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    transition {
      days          = 180
      storage_class = "GLACIER_IR"
    }
    expiration {
      days = var.bronze_retention_days # regulatory retention (e.g. 10 years = 3650)
    }
  }
}

resource "aws_s3_bucket_policy" "lake_tls_only" {
  bucket = aws_s3_bucket.lake.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource  = [aws_s3_bucket.lake.arn, "${aws_s3_bucket.lake.arn}/*"]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })
}
