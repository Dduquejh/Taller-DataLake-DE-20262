# Diseño: calidad, identificación, incrementalidad y recuperación

Este documento explica las decisiones del pipeline. Las columnas de cada tabla
están en el [diccionario de datos](diccionario_datos.md).

## Flujo de un lote

```
preflight_checks → extract_rss_to_bronze → prepare_silver_news → enrich_with_comprehend
  → publish_silver_news → build_gold_tables → validate_with_athena → record_run_status
```

| Tarea | Qué hace | Código |
|---|---|---|
| `preflight_checks` | Valida la configuración y el acceso: fuentes, archivo de validaciones, S3, Comprehend (una llamada mínima), las 6 tablas de Glue y el workgroup de Athena. Reporta todos los problemas juntos y no reintenta | `checks.preflight` |
| `extract_rss_to_bronze` | Consulta los 3 feeds y guarda cada XML válido y el manifiesto del lote | `ingestion.extract` |
| `prepare_silver_news` | Interpreta el XML, limpia, valida, manda los rechazos a cuarentena y deduplica dentro del lote. Deja el resultado en `silver/_staging/` | `silver.prepare_silver` |
| `enrich_with_comprehend` | Analiza solo los textos sin un análisis vigente. Guarda los resultados en `silver/_staging/`, así que un reintento no repite llamadas | `silver.enrich_silver` |
| `publish_silver_news` | Incorpora el lote al conjunto vigente (nuevas, actualizadas, repetidas) y reescribe solo las particiones afectadas | `silver.publish_silver` |
| `build_gold_tables` | Reconstruye solo las particiones de Gold desactualizadas respecto a Silver | `gold.build_gold` |
| `validate_with_athena` | Ejecuta `sql/06_validaciones_calidad.sql` y falla si alguna regla tiene incumplimientos | `checks.validate_with_athena` |
| `record_run_status` | Corre siempre, aunque falle una tarea anterior (`all_done`). Escribe `gold/_runs/run_id=…/status.json` y falla si el resultado no es `complete` | `checks.record_run_status` |

El paso 7 del enunciado, actualizar Glue, no es una tarea porque no hace falta:
las tablas usan *partition projection* (ver más abajo).

El DAG `noticias_bronze_ingestion` corre cada 3 horas. `noticias_bronze_reprocess`
es manual y cambia la extracción por `verify_existing_batch`, que verifica el
SHA-256 de cada XML guardado sin consultar los feeds. Ambos tienen
`max_active_runs=1`, y las tareas que escriben Silver y Gold usan el pool
`silver_writer`, de un cupo, para que los dos DAGs no escriban a la vez.

Los logs de cada etapa empiezan con la etapa, el lote y la fuente; por ejemplo
`LEIDA | <run_id> | eltiempo_medellin | 10 items | 0 rechazados` o
`SILVER | <run_id> | nuevas=… actualizadas=… repetidas=…`.

## Fuentes y estado del lote

Las fuentes están en `config/sources.yaml`. Cada consulta reintenta 3 veces los
errores de red y las respuestas 429 o 5xx. Una fuente se registra como `error`
si responde con un error HTTP, si la respuesta está vacía o si el XML está
malformado. Un XML válido sin `<item>` se acepta con 0 noticias y queda así
en el manifiesto (`news_count`). **Una fuente caída nunca cuenta como una fuente
sin noticias**: queda en el manifiesto con su motivo.

| Estado del lote | Condición | Qué hace el DAG |
|---|---|---|
| `complete` | Todas las fuentes se capturaron | Continúa; `record_run_status` en verde |
| `partial` | Al menos una se capturó y otra falló | Continúa con las fuentes buenas; `record_run_status` queda **en rojo** e indica la fuente fallida |
| `failed` | Ninguna se capturó | `extract_rss_to_bronze` falla y Airflow reintenta (`retries=2`) |

## Identificación de noticias

- `news_id = sha256(guid)`. Si el item no trae GUID, se usa
  `sha256(enlace normalizado)`. La normalización pasa el esquema y el dominio a
  minúsculas y quita el fragmento `#…`; la ruta y los parámetros se conservan.
- El GUID se prefiere porque es el identificador que el medio declara como
  estable. En El Tiempo es la URL canónica de la nota.

**Una noticia en varios feeds.** Si la misma noticia sale en dos secciones,
tiene el mismo `news_id`:
- dentro del lote se colapsa en una fila (cuenta en `duplicates_in_batch`);
- en Silver acumula todas sus secciones y feeds (`sections`, `source_ids`);
- en Gold cuenta una vez en cada sección (`news_daily`, `key_phrase_daily`) y
  una sola vez en `entity_daily`.

**Limitaciones:**
- Si el medio cambia el GUID o el enlace de una nota (por ejemplo, al
  corregir el titular en la URL), se trata como una noticia nueva y la
  anterior sigue vigente.
- Un mismo artículo con enlaces distintos, como parámetros de tracking
  (`?utm_…`), sin GUID, cuenta como noticias distintas, porque los
  parámetros no se quitan.
- No se detectan noticias casi iguales con distinto identificador: no hay
  deduplicación por similitud de texto.

## Reglas de calidad

**Al preparar Silver.** Un item que falla alguna regla va a
`quarantine/news/…/rejected.jsonl` con todos sus motivos, su posición y el
archivo de Bronze:

| Regla | Motivo |
|---|---|
| El título no está vacío después de limpiar el HTML | `titulo_vacio` |
| Hay enlace | `enlace_vacio` |
| El enlace es `http`/`https` con dominio | `enlace_invalido` |
| Hay fecha de publicación | `fecha_vacia` |
| La fecha se interpreta (RFC 822 o ISO 8601) y tiene zona horaria | `fecha_invalida` |

La descripción y el GUID son opcionales. Las fechas se guardan en UTC
(`published_at`) junto con el valor original (`pub_date_raw`).

**Al final de cada corrida, en Athena** (`sql/06_validaciones_calidad.sql`).
Cada regla debe dar 0:
- `news_id` único en `silver.news`;
- campos obligatorios presentes;
- enlaces http(s) válidos;
- `published_at` coincide con la partición y no está en el futuro;
- no hay entidades ni frases clave sin noticia asociada;
- no hay entidades duplicadas dentro de una noticia;
- `gold.news_daily` coincide con el recálculo desde `silver.news`
  (también en `sql/05_conciliacion_silver_gold.sql`).

## Registros recibidos y noticias únicas

Cada lote registra sus conteos en `silver/_batches/…/summary.json`, y
`status.json` los repite:

```
items_received = items_rejected + items_valid
items_valid    = unique_news + duplicates_in_batch
unique_news    = new + updated + repeated + stale
```

- **Recibidos**: todos los `<item>` de los XML del lote. Cada feed trae unos 10,
  así que un lote completo trae unos 30.
- **Rechazados**: los que fallan una regla y van a cuarentena.
- **Duplicados del lote**: la misma noticia en más de un feed del mismo lote.
- **Únicas**: noticias distintas del lote. Respecto al conjunto vigente, se
  clasifican así:
  - `new`: el `news_id` no existía;
  - `updated`: existía con otro `content_hash`, y la versión sube;
  - `repeated`: existía con el mismo contenido. Es lo más común, porque los
    feeds repiten sus últimas notas durante días;
  - `stale`: el lote es más antiguo que la versión vigente, por ejemplo al
    reprocesar un lote viejo. Se ignora para no revertir una actualización.

Por eso **un lote de 30 recibidos suele publicar 0 o pocas noticias nuevas**,
y el total de Silver crece mucho más despacio que la suma de los recibidos.

## Actualización de noticias

- Silver guarda **solo la versión vigente**: una fila por `news_id`, con su
  `version`. Las versiones anteriores no se conservan en las tablas; el XML
  de cada versión sigue en Bronze y la referencia está en `bronze_key`. Así
  ninguna versión antigua infla las métricas.
- Un cambio de título o descripción cambia `content_hash`: sube `version`,
  se actualizan `updated_at`, `last_run_id` y `bronze_key`, y la noticia
  queda pendiente de análisis.
- Si la noticia cambia de fecha de publicación, se reescriben las dos
  particiones: la de la fecha anterior y la nueva.

## Enriquecimiento con Comprehend

- Se analiza `title + "\n" + description`. Su hash es el mismo
  `content_hash`.
- **Sin llamadas repetidas**: una noticia solo se envía si `analyzed_text_hash`
  difiere de `content_hash`, o si su estado anterior fue `error` o `pending`.
  Los textos idénticos se envían una vez. Los resultados quedan en
  `silver/_staging/run_id=…/enrichment.json`, así que un reintento de la tarea
  no vuelve a llamar al servicio.
- Se usan las llamadas por lotes de 25 textos: detección de idioma, y luego
  entidades y frases clave en el idioma detectado.

| Estado | Cuándo | Se vuelve a analizar |
|---|---|---|
| `analyzed` | Análisis correcto | Solo si cambia el texto |
| `empty_text` | Texto vacío | Solo si cambia el texto |
| `too_long` | Más de 5000 bytes (límite de las llamadas por lotes) | Solo si cambia el texto |
| `unsupported_language` | Idioma distinto de `es` o `en`, o no detectado | Solo si cambia el texto |
| `error` | Comprehend devolvió error para ese texto | Sí, en el siguiente lote |
| `pending` | El texto cambió y no hay análisis disponible; se borran las entidades del texto anterior | Sí, en el siguiente lote |

**Limitaciones observadas** (útiles para la demo):
- "gobernador" sale como PERSON: es un cargo, no un nombre;
- "Medellín", "Santa Fe" y "Bucaramanga" salen como ORGANIZATION cuando el
  texto habla de los equipos, aunque también son lugares;
- "420" sale como LOCATION con confianza 0.39, y "antioqueño" como LOCATION.
  Por eso Gold filtra con `score` ≥ 0.5;
- las frases clave incluyen términos genéricos ("2026", "partido", "medida").
  Gold quita los términos solo numéricos y exige `score` ≥ 0.8, pero los
  genéricos con letras pasan;
- el análisis se hace solo sobre el titular y la descripción del RSS, no sobre
  el texto completo de la nota;
- no se resuelven alias: "Petro" y "Gustavo Petro" son entidades distintas.

## Incrementalidad

| Etapa | Qué se evita |
|---|---|
| Bronze | Un lote `complete` no se vuelve a consultar. Uno `partial` o `failed` solo vuelve a consultar las fuentes que fallaron y reutiliza los XML guardados |
| Silver | Solo se reescriben las particiones con noticias nuevas, actualizadas o con secciones nuevas. Un lote de repetidas escribe 0 particiones |
| Comprehend | Solo se envían los textos nuevos o modificados |
| Gold | Solo se reconstruye una partición si su archivo de Silver es más reciente que la marca de Gold (`news_daily`), si falta en Gold o si ya no existe en Silver. `{"full_gold": true}` en el reproceso fuerza la reconstrucción completa |

## Recuperación e idempotencia

- **Repetir un lote exitoso** (Clear de la corrida completa): la extracción ve
  el manifiesto `complete` y no consulta los feeds. Silver clasifica todo como
  repetido y no escribe; Comprehend recibe 0 textos; Gold no reconstruye nada.
- **Reprocesar desde Bronze** (`noticias_bronze_reprocess` con
  `{"run_id": "…"}`): verifica el SHA-256 de los XML guardados y vuelve a
  correr Silver, Comprehend y Gold, sin consultar los RSS. No crea otro
  manifiesto ni otra copia del lote. Un lote antiguo no revierte noticias más
  recientes, porque esas quedan como `stale`.
- **Fallo de una tarea**: cada etapa toma su entrada de S3 y no del estado en
  memoria, así que reintentarla da el mismo resultado:
  - `prepare` reescribe su staging;
  - `enrich` reutiliza los análisis guardados;
  - `publish` vuelve a leer el estado vigente y aplica el lote encima.

  Los errores transitorios se reintentan solos (`retries=2`, cada 5 minutos).
  Un error definitivo (preflight, validaciones, lote parcial) falla sin
  reintentos y se recupera con **Clear** sobre la tarea, marcando
  *Downstream*.
- **Fallo controlado** para demostrarlo: la Variable de Airflow
  `noticias_fail_task` con el `task_id` hace fallar esa tarea al empezar. Al
  borrarla, el reintento o el Clear la recuperan.
- **Lote parcial**: Clear sobre `extract_rss_to_bronze` con *Downstream*. Solo
  se vuelven a consultar las fuentes que fallaron, y el manifiesto se
  reemplaza con `IfMatch` sobre su ETag. El versionado del bucket conserva
  los intentos anteriores.

## Publicación sin resultados duplicados ni parciales en Athena

- **Cada archivo se reemplaza de forma atómica.** Cada partición es un solo
  objeto Parquet que se escribe con un `PutObject`. S3 no muestra objetos a
  medio escribir, así que Athena ve la versión anterior completa o la nueva
  completa. Un reintento reescribe el mismo archivo con la misma clave: no se
  agregan archivos, y por eso no puede haber filas duplicadas por reejecución.
- **Las tablas temporales no son visibles.** El staging (`silver/_staging/`),
  los resúmenes y los estados están fuera de las rutas de las tablas.
- **Orden de escritura.** Silver escribe primero las particiones que reciben
  noticias y después las que solo pierden: una interrupción a mitad de camino
  puede dejar una noticia temporalmente en dos fechas, pero nunca la pierde.
  En Gold, `news_daily` se escribe al final de cada partición y sirve de marca
  de que la partición quedó completa.

**Ventana de inconsistencia, aceptada y documentada.** Escribir varias
particiones no es una transacción. Si el proceso se corta entre dos archivos,
o mientras Silver ya publicó y Gold aún no:
- una noticia que cambió de fecha puede verse en dos particiones;
- Gold puede ir un paso atrás de Silver.

No se queda así. En la siguiente ejecución, `load_state` detecta la noticia
duplicada, conserva la versión más reciente y reescribe las dos particiones.
Gold reconstruye cualquier partición desactualizada. Mientras tanto,
`validate_with_athena` lo detecta (duplicados y conciliación Silver ↔ Gold), y
la corrida queda en rojo hasta que se recupera.

Para eliminar del todo esa ventana habría que publicar en una carpeta por
versión y cambiar la `LOCATION` de la tabla en Glue al final. Se descartó
porque, con *partition projection*, la plantilla de ubicación es fija y
obligaría a mantener una tabla por versión. Para el volumen del taller, unas
decenas de noticias por día con reescrituras de segundos, el riesgo es bajo.

## Glue Data Catalog y particiones

- Las 6 tablas se definen explícitamente con Terraform
  (`terraform/catalog/glue.tf`): ubicación, columnas y tipos, formato Parquet
  y la partición `published_date` (string `AAAA-MM-DD`). No se usan crawlers.
- **Particiones nuevas: partition projection.** Cada tabla declara
  `projection.published_date.type = date`, un rango desde `2026-01-01` hasta
  `NOW` y `storage.location.template = s3://…/published_date=${published_date}/`.
  Athena calcula las particiones a partir del filtro de la consulta. Una
  partición nueva escrita por el pipeline es visible de inmediato, sin
  `MSCK REPAIR`, sin crawler y sin `ALTER TABLE ADD PARTITION`. Una fecha sin
  carpeta devuelve 0 filas.
- Si cambia el esquema de Parquet en `silver.py` o `gold.py`, hay que
  actualizar `glue.tf`.
- Filtrar por `published_date` reduce lo escaneado
  (`sql/07_bytes_escaneados.sql`): en la prueba, la misma consulta escaneó
  11 387 bytes sin filtro y 2 011 con una fecha.
