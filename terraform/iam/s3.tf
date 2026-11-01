# Acceso del pipeline a los datos del bucket. La administración del bucket
# (crearlo, versionado, cifrado) queda fuera: la hace terraform/s3.
#
# Bronze no tiene permiso de borrado: los originales son inmutables y el
# reprocesamiento depende de ellos. Silver y Gold sí, porque se reescriben
# por partición y una partición que queda vacía se elimina.
data "aws_iam_policy_document" "s3" {
  statement {
    sid    = "ListDataLake"
    effect = "Allow"

    actions = [
      "s3:ListBucket",
      "s3:ListBucketVersions",
      "s3:GetBucketLocation",
    ]

    resources = [local.bucket_arn]
  }

  statement {
    sid    = "ReadWriteBronzeAndQuarantine"
    effect = "Allow"

    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion",
      "s3:PutObject",
    ]

    resources = [
      "${local.bucket_arn}/bronze/*",
      "${local.bucket_arn}/quarantine/*",
    ]
  }

  statement {
    sid    = "ReadWriteDeleteSilverAndGold"
    effect = "Allow"

    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion",
      "s3:PutObject",
      "s3:DeleteObject",
    ]

    resources = [
      "${local.bucket_arn}/silver/*",
      "${local.bucket_arn}/gold/*",
    ]
  }
}

resource "aws_iam_policy" "s3" {
  name        = "${var.project_name}-s3"
  description = "Lectura y escritura del pipeline en las zonas del data lake"
  policy      = data.aws_iam_policy_document.s3.json

  tags = local.common_tags
}

resource "aws_iam_user_policy_attachment" "pipeline_s3" {
  user       = var.pipeline_user_name
  policy_arn = aws_iam_policy.s3.arn
}
