variable "aws_region" {
  description = "Región AWS donde se desplegará el data lake"
  type        = string
  default     = "us-east-2"
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

