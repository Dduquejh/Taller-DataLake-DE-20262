data "aws_caller_identity" "current" {}

locals {
  # Mismo nombre que crea terraform/s3.
  bucket_name = "${var.project_name}-${data.aws_caller_identity.current.account_id}"
  bucket_arn  = "arn:aws:s3:::${local.bucket_name}"

  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}
