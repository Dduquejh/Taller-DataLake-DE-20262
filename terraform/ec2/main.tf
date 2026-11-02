data "aws_caller_identity" "current" {}

locals {
  # Mismo nombre que crea terraform/s3.
  bucket_name = "${var.project_name}-${data.aws_caller_identity.current.account_id}"

  # Mismos nombres que crea terraform/catalog.
  athena_workgroup = var.project_name
  glue_database    = replace(var.project_name, "-", "_")

  # Carpeta del repo en la instancia. Fuera del home para que ec2-user y
  # ssm-user (el usuario de Session Manager) la encuentren igual.
  app_dir = "/opt/${var.project_name}"

  # Los comandos de los outputs repiten el perfil con el que se aplicó, ya
  # que user_cli no tiene permisos de Session Manager.
  profile_flag = var.aws_profile == null ? "" : " --profile ${var.aws_profile}"

  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}

# Se usa la VPC por defecto: la instancia solo necesita salida a internet
# (RSS, GitHub, Docker Hub y las APIs de AWS) y una VPC propia con NAT
# costaría más que la instancia. La subred por defecto da IP pública, que
# es lo que permite esa salida sin NAT.
data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }

  filter {
    name   = "default-for-az"
    values = ["true"]
  }
}

# La AMI sale del parámetro público de SSM y no de un ID fijo: los IDs
# cambian por región y quedan obsoletos con cada parche.
data "aws_ssm_parameter" "al2023_ami" {
  name = var.ami_ssm_parameter
}
