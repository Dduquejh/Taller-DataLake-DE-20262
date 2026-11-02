# Terraform administra la salida y el SSH desde EC2 Instance Connect (abajo).
# Otras reglas de entrada, como el 8080 para una IP /32, se pueden agregar a
# mano: este recurso no declara bloques ingress en línea, así que un apply
# no las borra.
# Sin reglas de entrada, el UI sigue disponible con el túnel de Session
# Manager, que usa la conexión saliente del agente SSM.
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

# SSH solo desde el servicio EC2 Instance Connect de AWS, para el botón
# "Connect" de la consola: sube una llave temporal (60 s), así que la
# instancia no necesita par de llaves. El origen es la lista de prefijos que
# AWS mantiene para la región, no una IP ni 0.0.0.0/0.
data "aws_ec2_managed_prefix_list" "instance_connect" {
  name = "com.amazonaws.${var.aws_region}.ec2-instance-connect"
}

resource "aws_vpc_security_group_ingress_rule" "instance_connect" {
  security_group_id = aws_security_group.airflow.id
  description       = "SSH desde EC2 Instance Connect (consola)"
  ip_protocol       = "tcp"
  from_port         = 22
  to_port           = 22
  prefix_list_id    = data.aws_ec2_managed_prefix_list.instance_connect.id
}
