terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

# Crear el rol, el perfil de instancia y pasarlo a EC2 (iam:PassRole)
# requiere permisos que user_cli no tiene; aws_profile permite aplicar este
# stack con un perfil administrador.
provider "aws" {
  region  = var.aws_region
  profile = var.aws_profile
}
