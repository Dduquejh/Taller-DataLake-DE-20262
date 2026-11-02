# Consultas del pipeline sobre el catálogo: validaciones finales con Athena
# y consultas de análisis. Las tablas usan partition projection, así que el
# pipeline solo necesita leer el catálogo; no crea tablas ni particiones.
#
# Los nombres siguen a terraform/catalog: el workgroup se llama como el
# proyecto y la base de datos igual, con guiones bajos.
locals {
  glue_database = replace(var.project_name, "-", "_")
  glue_arn_base = "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}"
  athena_wg_arn = "arn:aws:athena:${var.aws_region}:${data.aws_caller_identity.current.account_id}:workgroup/${var.project_name}"
}

data "aws_iam_policy_document" "athena" {
  statement {
    sid    = "RunQueriesInWorkgroup"
    effect = "Allow"

    actions = [
      "athena:StartQueryExecution",
      "athena:StopQueryExecution",
      "athena:GetQueryExecution",
      "athena:GetQueryResults",
      "athena:GetQueryRuntimeStatistics",
      "athena:ListQueryExecutions",
      "athena:BatchGetQueryExecution",
      "athena:GetWorkGroup",
    ]

    resources = [local.athena_wg_arn]
  }

  statement {
    sid    = "ReadCatalog"
    effect = "Allow"

    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetTable",
      "glue:GetTables",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:BatchGetPartition",
    ]

    resources = [
      "${local.glue_arn_base}:catalog",
      "${local.glue_arn_base}:database/${local.glue_database}",
      "${local.glue_arn_base}:table/${local.glue_database}/*",
    ]
  }

  # Athena escribe los resultados con las credenciales de quien consulta.
  statement {
    sid    = "ReadWriteAthenaResults"
    effect = "Allow"

    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
    ]

    resources = ["${local.bucket_arn}/athena-results/*"]
  }

  statement {
    sid       = "ListResultsBucketUploads"
    effect    = "Allow"
    actions   = ["s3:ListBucketMultipartUploads"]
    resources = [local.bucket_arn]
  }
}

resource "aws_iam_policy" "athena" {
  name        = "${var.project_name}-athena"
  description = "Consultas del pipeline con Athena sobre el catálogo de Glue"
  policy      = data.aws_iam_policy_document.athena.json

  tags = local.common_tags
}

resource "aws_iam_group_policy_attachment" "pipeline_athena" {
  group      = aws_iam_group.pipeline.name
  policy_arn = aws_iam_policy.athena.arn
}
