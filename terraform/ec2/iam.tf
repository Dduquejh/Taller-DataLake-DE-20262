# Rol de la instancia: los contenedores obtienen credenciales temporales del
# IMDS en lugar de llaves de acceso copiadas al disco.
data "aws_iam_policy_document" "ec2_assume_role" {
  statement {
    sid     = "EC2AssumeRole"
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "airflow" {
  name               = "${var.project_name}-airflow-ec2"
  description        = "Rol de la instancia EC2 que ejecuta Airflow"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume_role.json

  tags = local.common_tags
}

# Las políticas del pipeline las crea y mantiene terraform/iam; aquí solo se
# leen por nombre. Así el rol de EC2 y el grupo de user_cli comparten los
# mismos permisos sin duplicarlos, y este stack no depende del state local
# de terraform/iam (terraform_remote_state ataría la ruta del archivo).
data "aws_iam_policy" "pipeline" {
  for_each = toset(["s3", "comprehend", "athena"])

  name = "${var.project_name}-${each.key}"
}

resource "aws_iam_role_policy_attachment" "pipeline" {
  for_each = data.aws_iam_policy.pipeline

  role       = aws_iam_role.airflow.name
  policy_arn = each.value.arn
}

# Session Manager reemplaza a SSH: da una terminal y el port forwarding del
# UI sin abrir puertos ni repartir llaves.
resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.airflow.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "airflow" {
  name = "${var.project_name}-airflow-ec2"
  role = aws_iam_role.airflow.name

  tags = local.common_tags
}
