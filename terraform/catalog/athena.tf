# Workgroup del pipeline y de las consultas de análisis.
#
# Los resultados quedan en el mismo bucket, bajo athena-results/, fuera de
# las zonas del data lake. terraform/s3 los borra a los pocos días con una
# regla de ciclo de vida.
#
# enforce_workgroup_configuration impide que un cliente cambie la ubicación
# de los resultados o desactive el límite de bytes escaneados.
resource "aws_athena_workgroup" "data_lake" {
  name          = var.project_name
  description   = "Consultas sobre las tablas de Silver y Gold"
  force_destroy = true

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = var.athena_bytes_scanned_cutoff

    engine_version {
      selected_engine_version = "Athena engine version 3"
    }

    result_configuration {
      output_location = "s3://${local.bucket_name}/${local.athena_results_prefix}"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }

  tags = local.common_tags
}
