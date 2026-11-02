variable "aws_region" {
  description = "Región AWS del data lake y de la instancia"
  type        = string
  default     = "us-east-2"
}

variable "aws_profile" {
  description = "Perfil de AWS CLI con permisos de IAM y EC2 para aplicar este stack"
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

# Airflow 3 levanta cinco procesos (api-server, scheduler, dag-processor,
# triggerer e init) más Postgres; con menos de 4 GiB de RAM el api-server y
# el build de la imagen se quedan sin memoria.
variable "instance_type" {
  description = "Tipo de instancia; t3.medium (2 vCPU, 4 GiB) es el mínimo cómodo para Airflow 3"
  type        = string
  default     = "t3.medium"
}

# "unlimited" es el valor por defecto de AWS para t3: evita que el build o
# un DAG pesado queden estrangulados, a cambio de un cobro extra solo si el
# uso promedio supera la línea base. "standard" nunca cobra de más.
variable "cpu_credits" {
  description = "Modo de créditos de CPU de la familia t (standard o unlimited)"
  type        = string
  default     = "unlimited"

  validation {
    condition     = contains(["standard", "unlimited"], var.cpu_credits)
    error_message = "cpu_credits debe ser \"standard\" o \"unlimited\"."
  }
}

# La imagen de Airflow (~2 GiB), la capa de build, Postgres y los logs
# caben con holgura en 30 GiB; 8 GiB (el mínimo de la AMI) se llena.
variable "root_volume_size" {
  description = "Tamaño en GiB del disco raíz gp3"
  type        = number
  default     = 30
}

# Parámetro público de SSM que siempre apunta a la última AMI de Amazon
# Linux 2023. Para Graviton (t4g) usar el sufijo arm64.
variable "ami_ssm_parameter" {
  description = "Parámetro de SSM con el ID de la AMI de Amazon Linux 2023"
  type        = string
  default     = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}

# El repo es público, así que se clona por HTTPS sin llaves de despliegue.
# La rama debe existir en GitHub: el trabajo local sin push no llega a EC2.
variable "repo_url" {
  description = "URL HTTPS del repositorio a clonar en la instancia"
  type        = string
  default     = "https://github.com/Dduquejh/Taller-DataLake-DE-20262.git"
}

variable "repo_branch" {
  description = "Rama del repositorio que se despliega"
  type        = string
  default     = "main"
}

# Amazon Linux 2023 trae docker pero no los plugins compose ni buildx, y
# compose necesita buildx para construir la imagen. Se fijan las versiones
# para que dos despliegues den el mismo resultado.
variable "docker_compose_version" {
  description = "Versión del plugin docker compose (release de GitHub)"
  type        = string
  default     = "v5.6.1"
}

variable "docker_buildx_version" {
  description = "Versión del plugin docker buildx (release de GitHub)"
  type        = string
  default     = "v0.38.1"
}

# Las reglas de entrada (8080 para el UI, 22 para SSH) no las maneja este
# stack: se agregan a mano en el security group, restringidas a la IP de
# quien las necesita, y Terraform no las toca (ver network.tf).
#
# key_name solo se usa al crear la instancia: cambiarlo la recrea. Sin par
# de llaves se puede entrar por Session Manager o por EC2 Instance Connect.
variable "key_name" {
  description = "Par de llaves EC2 existente para SSH (opcional, solo al crear la instancia)"
  type        = string
  default     = null
}
