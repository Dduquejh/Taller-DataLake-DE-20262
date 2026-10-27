output "bucket_name" {
  description = "Nombre del bucket del data lake"
  value       = aws_s3_bucket.data_lake.bucket
}

output "bucket_arn" {
  description = "ARN del bucket del data lake"
  value       = aws_s3_bucket.data_lake.arn
}

output "aws_region" {
  description = "Región del bucket"
  value       = var.aws_region
}

