variable "aws_region" {
  description = "Región AWS donde se usará Comprehend"
  type        = string
  default     = "us-east-2"
}

variable "aws_profile" {
  description = "Perfil de AWS CLI con permisos de IAM para aplicar este stack"
  type        = string
  default     = null
}

variable "project_name" {
  description = "Prefijo para nombrar los recursos"
  type        = string
  default     = "noticias-data-lake"
}

variable "environment" {
  description = "Entorno de despliegue"
  type        = string
  default     = "dev"
}

variable "pipeline_user_name" {
  description = "Usuario IAM con el que corre el pipeline en local"
  type        = string
  default     = "user_cli"
}
