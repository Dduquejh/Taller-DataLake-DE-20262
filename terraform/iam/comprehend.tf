# Las detecciones síncronas de Comprehend usan modelos integrados y no
# admiten permisos por recurso, por eso Resource es "*". La condición de
# región limita el uso a la región del data lake.
#
# No se crea un rol de acceso a datos para Comprehend: solo lo necesitan
# los trabajos asíncronos que leen de S3, y el pipeline envía el texto en
# la propia solicitud.
data "aws_iam_policy_document" "comprehend" {
  statement {
    sid    = "DetectEntitiesAndKeyPhrases"
    effect = "Allow"

    actions = [
      "comprehend:BatchDetectEntities",
      "comprehend:BatchDetectKeyPhrases",
      "comprehend:BatchDetectDominantLanguage",
      "comprehend:DetectEntities",
      "comprehend:DetectKeyPhrases",
      "comprehend:DetectDominantLanguage",
    ]

    resources = ["*"]

    condition {
      test     = "StringEquals"
      variable = "aws:RequestedRegion"
      values   = [var.aws_region]
    }
  }
}

resource "aws_iam_policy" "comprehend" {
  name        = "${var.project_name}-comprehend"
  description = "Enriquecimiento de noticias con Amazon Comprehend"
  policy      = data.aws_iam_policy_document.comprehend.json

  tags = local.common_tags
}

resource "aws_iam_user_policy_attachment" "pipeline_comprehend" {
  user       = var.pipeline_user_name
  policy_arn = aws_iam_policy.comprehend.arn
}
