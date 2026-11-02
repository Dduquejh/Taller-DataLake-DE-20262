data "aws_caller_identity" "current" {}

locals {
  # Mismo nombre que crea terraform/s3.
  bucket_name = "${var.project_name}-${data.aws_caller_identity.current.account_id}"

  # Glue y Athena no aceptan guiones en los nombres de base de datos.
  database_name = replace(var.project_name, "-", "_")

  athena_results_prefix = "athena-results/"

  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}
