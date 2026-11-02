output "glue_database" {
  description = "Base de datos del catálogo (ATHENA_DATABASE)"
  value       = aws_glue_catalog_database.data_lake.name
}

output "glue_tables" {
  description = "Tablas registradas en el catálogo"
  value       = sort(keys(aws_glue_catalog_table.tables))
}

output "athena_workgroup" {
  description = "Workgroup de Athena (ATHENA_WORKGROUP)"
  value       = aws_athena_workgroup.data_lake.name
}

output "athena_results_location" {
  description = "Ubicación de los resultados de Athena"
  value       = aws_athena_workgroup.data_lake.configuration[0].result_configuration[0].output_location
}
