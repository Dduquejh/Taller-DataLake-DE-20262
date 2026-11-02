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

# Las políticas del pipeline se asignan a un grupo y no directamente al
# usuario: user_cli ya tiene el máximo de 10 políticas administradas
# (varias de otros proyectos). Un grupo admite otras 10 y deja juntas las
# del taller. La membresía no es exclusiva: no toca otros grupos del usuario.
resource "aws_iam_group" "pipeline" {
  name = "${var.project_name}-pipeline"
}

resource "aws_iam_user_group_membership" "pipeline" {
  user   = var.pipeline_user_name
  groups = [aws_iam_group.pipeline.name]
}
