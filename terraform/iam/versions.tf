terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

# Crear y adjuntar políticas requiere permisos de IAM que user_cli no
# tiene; aws_profile permite aplicar este stack con un perfil administrador.
provider "aws" {
  region  = var.aws_region
  profile = var.aws_profile
}
