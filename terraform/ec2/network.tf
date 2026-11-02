# Sin reglas de ingreso por defecto: Session Manager funciona con conexiones
# salientes del agente SSM, así que el UI se alcanza sin abrir puertos.
resource "aws_security_group" "airflow" {
  name        = "${var.project_name}-airflow"
  description = "Airflow en EC2: sin ingreso por defecto, salida libre"
  vpc_id      = data.aws_vpc.default.id

  tags = local.common_tags
}

# La salida es libre porque los destinos (feeds RSS, GitHub, Docker Hub,
# PyPI y las APIs de AWS) son muchos y cambian de IP.
resource "aws_vpc_security_group_egress_rule" "all" {
  security_group_id = aws_security_group.airflow.id
  description       = "Salida a internet"
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"
}

resource "aws_vpc_security_group_ingress_rule" "airflow_ui" {
  count = var.allowed_cidr == null ? 0 : 1

  security_group_id = aws_security_group.airflow.id
  description       = "UI de Airflow desde una IP autorizada"
  ip_protocol       = "tcp"
  from_port         = 8080
  to_port           = 8080
  cidr_ipv4         = var.allowed_cidr
}

resource "aws_vpc_security_group_ingress_rule" "ssh" {
  count = var.ssh_allowed_cidr == null ? 0 : 1

  security_group_id = aws_security_group.airflow.id
  description       = "SSH desde una IP autorizada"
  ip_protocol       = "tcp"
  from_port         = 22
  to_port           = 22
  cidr_ipv4         = var.ssh_allowed_cidr
}
