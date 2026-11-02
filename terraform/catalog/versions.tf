terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

# Crear el catálogo y el workgroup requiere permisos de Glue y Athena que
# user_cli no tiene; aws_profile permite aplicarlo con un perfil administrador.
provider "aws" {
  region  = var.aws_region
  profile = var.aws_profile
}
