# Catálogo de las tablas de Silver y Gold.
#
# Las columnas reflejan los esquemas de pyarrow de src/pipeline/silver.py y
# src/pipeline/gold.py; si cambian allá, hay que cambiarlas aquí.
#
# Particiones: partition projection sobre published_date. Athena calcula las
# particiones a partir del rango de fechas y de storage.location.template,
# así que una partición nueva escrita por el pipeline queda visible de
# inmediato, sin MSCK REPAIR, sin crawlers y sin una tarea en el DAG que
# registre particiones. Una fecha del rango sin carpeta en S3 simplemente no
# devuelve filas.

resource "aws_glue_catalog_database" "data_lake" {
  name        = local.database_name
  description = "Noticias RSS: tablas de Silver y Gold del data lake"

  tags = local.common_tags
}

locals {
  silver_news_columns = [
    { name = "news_id", type = "string", comment = "Identificador estable: GUID o, si falta, el enlace" },
    { name = "guid", type = "string", comment = "GUID del item RSS" },
    { name = "link", type = "string", comment = "Enlace de la noticia" },
    { name = "title", type = "string", comment = "Título sin HTML" },
    { name = "description", type = "string", comment = "Descripción sin HTML" },
    { name = "medium", type = "string", comment = "Medio de comunicación" },
    { name = "sections", type = "array<string>", comment = "Secciones en las que apareció la noticia" },
    { name = "source_ids", type = "array<string>", comment = "Feeds en los que apareció la noticia" },
    { name = "pub_date_raw", type = "string", comment = "Fecha de publicación original del RSS" },
    { name = "published_at", type = "timestamp", comment = "Fecha de publicación en UTC" },
    { name = "content_hash", type = "string", comment = "SHA-256 de título + descripción" },
    { name = "version", type = "int", comment = "Versión; sube cuando cambia content_hash" },
    { name = "first_seen_at", type = "timestamp", comment = "Primera vez que se vio la noticia (UTC)" },
    { name = "updated_at", type = "timestamp", comment = "Último cambio de contenido (UTC)" },
    { name = "first_run_id", type = "string", comment = "Lote en el que apareció por primera vez" },
    { name = "last_run_id", type = "string", comment = "Último lote que la actualizó" },
    { name = "bronze_key", type = "string", comment = "Archivo de Bronze del que proviene" },
    { name = "processed_at", type = "timestamp", comment = "Momento del procesamiento en Silver (UTC)" },
    { name = "analysis_status", type = "string", comment = "analyzed, empty_text, too_long, unsupported_language, error o pending" },
    { name = "analysis_language", type = "string", comment = "Idioma detectado por Comprehend" },
    { name = "analysis_language_score", type = "double", comment = "Confianza del idioma detectado" },
    { name = "analysis_error", type = "string", comment = "Error de Comprehend, si lo hubo" },
    { name = "analyzed_text", type = "string", comment = "Texto enviado a Comprehend" },
    { name = "analyzed_text_hash", type = "string", comment = "Hash del texto analizado" },
    { name = "analyzed_at", type = "timestamp", comment = "Momento del análisis (UTC)" },
  ]

  silver_entities_columns = [
    { name = "news_id", type = "string", comment = "Noticia a la que pertenece la entidad" },
    { name = "entity_text", type = "string", comment = "Texto de la entidad tal como lo devolvió Comprehend" },
    { name = "entity_text_normalized", type = "string", comment = "Texto normalizado para agrupar" },
    { name = "entity_type", type = "string", comment = "PERSON, ORGANIZATION o LOCATION" },
    { name = "score", type = "double", comment = "Confianza máxima entre las menciones" },
    { name = "mention_count", type = "int", comment = "Menciones en la noticia" },
    { name = "text_hash", type = "string", comment = "Hash del texto analizado" },
    { name = "language", type = "string", comment = "Idioma usado en el análisis" },
    { name = "analyzed_at", type = "timestamp", comment = "Momento del análisis (UTC)" },
  ]

  silver_key_phrases_columns = [
    { name = "news_id", type = "string", comment = "Noticia a la que pertenece la frase" },
    { name = "key_phrase", type = "string", comment = "Frase clave tal como la devolvió Comprehend" },
    { name = "key_phrase_normalized", type = "string", comment = "Frase normalizada para agrupar" },
    { name = "score", type = "double", comment = "Confianza máxima entre las menciones" },
    { name = "mention_count", type = "int", comment = "Menciones en la noticia" },
    { name = "text_hash", type = "string", comment = "Hash del texto analizado" },
    { name = "language", type = "string", comment = "Idioma usado en el análisis" },
    { name = "analyzed_at", type = "timestamp", comment = "Momento del análisis (UTC)" },
  ]

  gold_news_daily_columns = [
    { name = "medium", type = "string", comment = "Medio de comunicación" },
    { name = "section", type = "string", comment = "Sección" },
    { name = "news_count", type = "bigint", comment = "Noticias vigentes publicadas ese día" },
  ]

  gold_entity_daily_columns = [
    { name = "entity_type", type = "string", comment = "PERSON, ORGANIZATION o LOCATION" },
    { name = "entity", type = "string", comment = "Entidad normalizada (clave de agrupación)" },
    { name = "entity_text", type = "string", comment = "Forma más frecuente de la entidad, para mostrar" },
    { name = "news_count", type = "bigint", comment = "Noticias distintas que la mencionan" },
    { name = "mention_count", type = "bigint", comment = "Total de menciones" },
    { name = "max_score", type = "double", comment = "Confianza máxima" },
  ]

  gold_key_phrase_daily_columns = [
    { name = "section", type = "string", comment = "Sección" },
    { name = "key_phrase", type = "string", comment = "Frase normalizada (clave de agrupación)" },
    { name = "key_phrase_text", type = "string", comment = "Forma más frecuente de la frase, para mostrar" },
    { name = "news_count", type = "bigint", comment = "Noticias distintas que la contienen" },
  ]

  tables = {
    news = {
      prefix      = "silver/news"
      description = "Silver: una fila por noticia vigente, con su enriquecimiento"
      columns     = local.silver_news_columns
    }
    news_entities = {
      prefix      = "silver/news_entities"
      description = "Silver: entidades PERSON, ORGANIZATION y LOCATION por noticia"
      columns     = local.silver_entities_columns
    }
    news_key_phrases = {
      prefix      = "silver/news_key_phrases"
      description = "Silver: frases clave por noticia"
      columns     = local.silver_key_phrases_columns
    }
    news_daily = {
      prefix      = "gold/news_daily"
      description = "Gold: noticias por día, medio y sección"
      columns     = local.gold_news_daily_columns
    }
    entity_daily = {
      prefix      = "gold/entity_daily"
      description = "Gold: noticias y menciones por día y entidad"
      columns     = local.gold_entity_daily_columns
    }
    key_phrase_daily = {
      prefix      = "gold/key_phrase_daily"
      description = "Gold: noticias por día, sección y frase clave"
      columns     = local.gold_key_phrase_daily_columns
    }
  }
}

resource "aws_glue_catalog_table" "tables" {
  for_each = local.tables

  name          = each.key
  database_name = aws_glue_catalog_database.data_lake.name
  description   = each.value.description
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL                                  = "TRUE"
    classification                            = "parquet"
    "parquet.compression"                     = "SNAPPY"
    "projection.enabled"                      = "true"
    "projection.published_date.type"          = "date"
    "projection.published_date.format"        = "yyyy-MM-dd"
    "projection.published_date.range"         = "${var.projection_start_date},NOW"
    "projection.published_date.interval"      = "1"
    "projection.published_date.interval.unit" = "DAYS"
    "storage.location.template"               = "s3://${local.bucket_name}/${each.value.prefix}/published_date=$${published_date}/"
  }

  # Con projection.published_date.type = "date", Athena admite filtros como
  # published_date = '2026-10-31' o BETWEEN sobre la columna como string.
  partition_keys {
    name    = "published_date"
    type    = "string"
    comment = "Fecha de publicación en UTC (AAAA-MM-DD)"
  }

  storage_descriptor {
    location      = "s3://${local.bucket_name}/${each.value.prefix}/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters = {
        "serialization.format" = "1"
      }
    }

    dynamic "columns" {
      for_each = each.value.columns

      content {
        name    = columns.value.name
        type    = columns.value.type
        comment = columns.value.comment
      }
    }
  }
}
