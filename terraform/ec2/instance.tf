resource "aws_instance" "airflow" {
  # insecure_value y no value: el parámetro de la AMI es público y de tipo
  # String; con value el plan ocultaría el ID como sensible.
  ami           = data.aws_ssm_parameter.al2023_ami.insecure_value
  instance_type = var.instance_type

  # Orden fijo para que un plan posterior no cambie de subred (y recree la
  # instancia) solo porque la API devolvió los IDs en otro orden.
  subnet_id                   = sort(data.aws_subnets.default.ids)[0]
  vpc_security_group_ids      = [aws_security_group.airflow.id]
  associate_public_ip_address = true
  iam_instance_profile        = aws_iam_instance_profile.airflow.name

  key_name = var.key_name

  # cloud-init ejecuta user_data solo en el primer arranque. Un cambio en la
  # plantilla se aplica a las instancias nuevas, pero no recrea la actual
  # (ver ignore_changes): así un ajuste del script no borra Postgres en
  # medio de la demo. Para aplicarlo a la instancia actual:
  #   terraform apply -replace=aws_instance.airflow ...
  user_data = templatefile("${path.module}/user_data.sh.tftpl", {
    app_dir                = local.app_dir
    repo_url               = var.repo_url
    repo_branch            = var.repo_branch
    aws_region             = var.aws_region
    bucket_name            = local.bucket_name
    athena_workgroup       = local.athena_workgroup
    glue_database          = local.glue_database
    docker_compose_version = var.docker_compose_version
    docker_buildx_version  = var.docker_buildx_version
    compose_override       = file("${path.module}/docker-compose.ec2.yaml")
  })

  # IMDSv2 obligatorio: el token por PUT evita que un SSRF en un DAG o en un
  # feed robe las credenciales del rol con un simple GET.
  #
  # El hop limit sube de 1 a 2 porque Airflow corre en contenedores con red
  # bridge: la respuesta del IMDS atraviesa el host y el puente de Docker,
  # que cuenta como un salto más. Con 1 el token nunca llega al contenedor y
  # boto3 se queda sin credenciales.
  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 2
    instance_metadata_tags      = "disabled"
  }

  root_block_device {
    volume_type           = "gp3"
    volume_size           = var.root_volume_size
    encrypted             = true
    delete_on_termination = true
  }

  credit_specification {
    cpu_credits = var.cpu_credits
  }

  tags        = merge(local.common_tags, { Name = "${var.project_name}-airflow" })
  volume_tags = merge(local.common_tags, { Name = "${var.project_name}-airflow" })

  lifecycle {
    # El parámetro de SSM apunta a una AMI nueva con cada parche; sin esto,
    # cualquier apply posterior recrearía la instancia y borraría Postgres.
    # user_data: ver el comentario de arriba.
    ignore_changes = [ami, user_data]
  }
}
