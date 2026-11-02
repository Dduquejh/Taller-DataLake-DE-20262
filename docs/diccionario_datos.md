# Diccionario de datos

Todo vive en un solo bucket, `s3://noticias-data-lake-<ACCOUNT_ID>/`. Silver y
Gold están en Parquet (Snappy), con **un archivo por partición**, y se
particionan por `published_date`: la fecha de publicación en UTC, como texto
`AAAA-MM-DD`. Esa columna no se guarda dentro de los archivos; Athena la toma
de la ruta mediante *partition projection* (ver [diseño](diseno.md#glue-data-catalog-y-particiones)).

Las tablas están registradas en Glue (`terraform/catalog/glue.tf`), en la base
de datos `noticias_data_lake`, y cada columna tiene allí su comentario.

| Zona | Ruta | Formato |
|---|---|---|
| Bronze | `bronze/source=<fuente>/ingestion_date=AAAA-MM-DD/run_id=<id>/response.xml` | XML original, sin modificar |
| Bronze | `bronze/manifests/ingestion_date=AAAA-MM-DD/run_id=<id>/manifest.json` | JSON |
| Cuarentena | `quarantine/news/ingestion_date=AAAA-MM-DD/run_id=<id>/rejected.jsonl` | JSON Lines |
| Silver | `silver/<tabla>/published_date=AAAA-MM-DD/<tabla>.parquet` | Parquet |
| Gold | `gold/<tabla>/published_date=AAAA-MM-DD/<tabla>.parquet` | Parquet |
| Control | `silver/_batches/…/summary.json`, `gold/_builds/…/summary.json`, `gold/_runs/run_id=<id>/status.json` | JSON |
| Temporal | `silver/_staging/run_id=<id>/` (entre tareas del DAG) y `athena-results/` (se borra a los 7 días) | JSON / CSV |

## Bronze

### `response.xml`

El cuerpo de la respuesta HTTP tal como llegó. Se escribe con
`IfNoneMatch="*"`, así que un reintento nunca lo sobrescribe, y el pipeline no
tiene permiso para borrar en `bronze/`.

### `manifest.json`: un archivo por lote

| Campo | Descripción |
|---|---|
| `run_id` | Identificador del lote (el `run_id` de la corrida de Airflow que lo extrajo) |
| `ingestion_date` | Fecha UTC de la ingesta; es parte de la ruta |
| `started_at`, `finished_at` | Inicio y fin de la extracción (UTC) |
| `operation` | `extract` (RSS real) o `scenario` (lote controlado de `scripts/scenarios.py`) |
| `status` | `complete` (todas las fuentes bien), `partial` (al menos una bien y otra falló) o `failed` (ninguna) |
| `attempt`, `previous_status` | Número de intento y estado anterior, cuando se recupera un lote parcial o fallido |
| `success_count`, `error_count` | Fuentes capturadas y fallidas |
| `sources[]` | Una entrada por fuente, descrita en la tabla siguiente |

| Campo de `sources[]` | Descripción |
|---|---|
| `source_id`, `medium`, `section`, `url` | La fuente, tomada de `config/sources.yaml` |
| `fetched_at` | Momento de la consulta (UTC) |
| `status` | `success` o `error` |
| `http_status`, `content_type` | Respuesta HTTP |
| `error` | Motivo del error: HTTP, respuesta vacía, XML malformado, red… |
| `sha256`, `bytes`, `news_count` | Hash, tamaño e items del XML guardado |
| `s3_key` | Ubicación del XML en Bronze |
| `reused_existing_object` | `true` si un reintento encontró el XML ya guardado y lo reutilizó |

## Cuarentena: `rejected.jsonl`

Una línea por item rechazado, con trazabilidad hasta Bronze.

| Campo | Descripción |
|---|---|
| `run_id`, `ingestion_date`, `source_id` | Lote y fuente |
| `bronze_key`, `item_index` | Archivo de Bronze y posición del `<item>` dentro de él |
| `reasons` | Lista de motivos: `titulo_vacio`, `enlace_vacio`, `enlace_invalido`, `fecha_vacia`, `fecha_invalida` |
| `raw` | `guid`, `title`, `link`, `description` y `pub_date_raw` tal como venían |

## Silver

### `news`: una fila por noticia vigente

| Columna | Tipo | Descripción |
|---|---|---|
| `news_id` | string | SHA-256 del GUID o, si no hay GUID, del enlace normalizado. Es único en la tabla |
| `guid` | string | GUID del item RSS |
| `link` | string | Enlace normalizado: esquema y dominio en minúsculas, sin fragmento `#` |
| `title`, `description` | string | Texto sin HTML, con entidades decodificadas y espacios colapsados |
| `medium` | string | Medio (El Tiempo) |
| `sections` | array\<string\> | Todas las secciones en que apareció la noticia |
| `source_ids` | array\<string\> | Todos los feeds en que apareció |
| `pub_date_raw` | string | Fecha de publicación original del RSS |
| `published_at` | timestamp | Fecha de publicación en UTC |
| `content_hash` | string | SHA-256 de `title + "\n" + description`; detecta los cambios |
| `version` | int | 1 al aparecer; sube cada vez que cambia `content_hash` |
| `first_seen_at`, `first_run_id` | timestamp, string | Primera captura y su lote |
| `updated_at`, `last_run_id` | timestamp, string | Captura de la versión vigente y su lote |
| `bronze_key` | string | XML de Bronze del que sale la versión vigente |
| `processed_at` | timestamp | Última vez que Silver modificó la fila |
| `analysis_status` | string | `analyzed`, `empty_text`, `too_long`, `unsupported_language`, `error` o `pending` |
| `analysis_language`, `analysis_language_score` | string, double | Idioma detectado por Comprehend y su confianza |
| `analysis_error` | string | Error de Comprehend, si lo hubo |
| `analyzed_text`, `analyzed_text_hash` | string | Texto enviado a Comprehend y su hash. Si el hash coincide con `content_hash`, el análisis está vigente |
| `analyzed_at` | timestamp | Momento del análisis |

### `news_entities`: una fila por entidad distinta en cada noticia vigente

| Columna | Tipo | Descripción |
|---|---|---|
| `news_id` | string | Noticia en `news`, en la misma partición |
| `entity_text` | string | Forma devuelta por Comprehend |
| `entity_text_normalized` | string | Forma normalizada (minúsculas, sin puntuación de borde); la entidad es única por (`news_id`, `entity_type`, normalizada) |
| `entity_type` | string | Solo `PERSON`, `ORGANIZATION` o `LOCATION` |
| `score` | double | Confianza máxima entre sus menciones en el texto |
| `mention_count` | int | Menciones en el texto |
| `text_hash`, `language`, `analyzed_at` | | Del análisis que la produjo |

### `news_key_phrases`: una fila por frase clave distinta en cada noticia vigente

Tiene las mismas columnas que `news_entities`, con `key_phrase` y
`key_phrase_normalized` en lugar de la entidad y sin `entity_type`.

## Gold

Todas las métricas cuentan **noticias distintas**: una entidad mencionada tres
veces en un texto suma una sola noticia. Cada noticia vigente está en una sola
fecha, así que sumar días no cuenta dos veces la misma noticia.

### `news_daily`: día × medio × sección

| Columna | Tipo | Definición |
|---|---|---|
| `medium`, `section` | string | |
| `news_count` | bigint | Noticias vigentes publicadas ese día en esa sección. Una noticia con dos secciones cuenta en cada una, así que la suma por sección puede superar el total de noticias únicas |

### `entity_daily`: día × tipo × entidad

| Columna | Tipo | Definición |
|---|---|---|
| `entity_type` | string | `PERSON`, `ORGANIZATION` o `LOCATION` |
| `entity` | string | Clave de agrupación: la forma limpia en minúsculas |
| `entity_text` | string | Forma escrita más frecuente, para mostrar |
| `news_count` | bigint | **Presencia**: noticias distintas que mencionan la entidad |
| `mention_count` | bigint | Total de menciones (informativo; no es la medida de presencia) |
| `max_score` | double | Confianza máxima |

Solo entran entidades con `score` ≥ 0.5. Se descartan los términos sin letras
(solo numéricos), los artículos y los interrogativos.

### `key_phrase_daily`: día × sección × frase

| Columna | Tipo | Definición |
|---|---|---|
| `section` | string | Cada sección de la noticia |
| `key_phrase` | string | Clave de agrupación, sin artículos ni preposiciones al inicio o al final |
| `key_phrase_text` | string | Forma escrita más frecuente |
| `news_count` | bigint | Noticias distintas de esa sección que contienen la frase |

Solo entran frases con `score` ≥ 0.8.

### Noticias sin fecha de publicación válida

La fecha de publicación es obligatoria. Un item sin `pubDate`, con una fecha
ilegible o sin zona horaria va a cuarentena (`fecha_vacia` o
`fecha_invalida`) y no llega a Silver ni a Gold. Así, toda noticia de Gold
tiene exactamente una fecha y los conteos diarios no dependen de una fecha
supuesta.

## Registros de control

| Archivo | Contenido |
|---|---|
| `silver/_batches/ingestion_date=…/run_id=…/summary.json` | Conteos del lote: `items_received`, `items_rejected`, `items_valid`, `duplicates_in_batch`, `unique_news`, `new`, `updated`, `repeated`, `stale`, particiones escritas, resultados de Comprehend y totales vigentes |
| `gold/_builds/run_id=…/summary.json` | Particiones de Gold reconstruidas y sus filas, más los umbrales usados |
| `gold/_runs/run_id=…/status.json` | Estado final de la corrida (`complete`, `partial` o `failed`), estado del lote, estado de cada etapa y sus resultados, incluidas las validaciones de Athena |
