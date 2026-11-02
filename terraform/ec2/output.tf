output "instance_id" {
  description = "ID de la instancia de Airflow"
  value       = aws_instance.airflow.id
}

# Cambia en cada stop/start: la IP pública automática no se conserva.
output "public_ip" {
  description = "IP pública actual de la instancia"
  value       = aws_instance.airflow.public_ip
}

output "role_name" {
  description = "Rol IAM de la instancia"
  value       = aws_iam_role.airflow.name
}

output "attached_policies" {
  description = "Políticas del taller adjuntas al rol"
  value       = sort([for p in data.aws_iam_policy.pipeline : p.name])
}

# Requiere el Session Manager plugin instalado en la máquina local.
output "ssm_session_command" {
  description = "Terminal en la instancia por Session Manager"
  value       = "aws ssm start-session --target ${aws_instance.airflow.id} --region ${var.aws_region}${local.profile_flag}"
}

# Forma recomendada de abrir el UI: el puerto 8080 queda cerrado en el
# security group y el túnel viaja por la conexión saliente del agente SSM.
output "ssm_port_forward_command" {
  description = "Túnel al UI de Airflow; luego abrir http://localhost:8080"
  value       = "aws ssm start-session --target ${aws_instance.airflow.id} --region ${var.aws_region}${local.profile_flag} --document-name AWS-StartPortForwardingSession --parameters '{\"portNumber\":[\"8080\"],\"localPortNumber\":[\"8080\"]}'"
}

output "airflow_url" {
  description = "URL del UI de Airflow"
  value       = var.allowed_cidr == null ? "http://localhost:8080 (con el túnel de ssm_port_forward_command)" : "http://${aws_instance.airflow.public_ip}:8080"
}

# Se ejecuta dentro de la sesión SSM. El usuario es admin; la contraseña
# cambia si se recrea el contenedor del api-server.
output "airflow_password_command" {
  description = "Muestra la contraseña del usuario admin de Airflow (dentro de la sesión SSM)"
  value       = "cd ${local.app_dir} && sudo docker compose exec airflow-apiserver cat /opt/airflow/simple_auth_manager_passwords.json.generated"
}

output "bootstrap_log_command" {
  description = "Sigue el arranque (user_data) dentro de la sesión SSM"
  value       = "sudo tail -f /var/log/cloud-init-output.log"
}
