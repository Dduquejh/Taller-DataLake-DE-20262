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


variable "athena_results_retention_days" {
  description = "Días que se conservan los resultados de Athena antes de borrarlos"
  type        = number
  default     = 7
}
