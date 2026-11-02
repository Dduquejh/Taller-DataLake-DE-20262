variable "aws_region" {
  description = "Región AWS del data lake"
  type        = string
  default     = "us-east-2"
}

variable "aws_profile" {
  description = "Perfil de AWS CLI con permisos de Glue y Athena para aplicar este stack"
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

variable "projection_start_date" {
  description = "Primera fecha (AAAA-MM-DD) que Athena considera en las particiones proyectadas"
  type        = string
  default     = "2026-01-01"
}

variable "athena_bytes_scanned_cutoff" {
  description = "Máximo de bytes que puede escanear una consulta del workgroup"
  type        = number
  default     = 1073741824 # 1 GiB
}
