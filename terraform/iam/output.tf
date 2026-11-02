output "comprehend_policy_arn" {
  description = "ARN de la política de Comprehend; se reutiliza en el rol de EC2"
  value       = aws_iam_policy.comprehend.arn
}

output "s3_policy_arn" {
  description = "ARN de la política de S3; se reutiliza en el rol de EC2"
  value       = aws_iam_policy.s3.arn
}

output "athena_policy_arn" {
  description = "ARN de la política de Athena y Glue; se reutiliza en el rol de EC2"
  value       = aws_iam_policy.athena.arn
}
