"""
Capa Silver: convierte los RSS originales de Bronze en noticias normalizadas
y enriquecidas con Amazon Comprehend.

Se ejecuta en tres etapas, cada una una tarea del DAG:

1. prepare: limpia, valida y deduplica el lote; guarda el resultado en
   silver/_staging/ y los rechazos en quarantine/.
2. enrich: analiza con Comprehend solo los textos nuevos o modificados y
   guarda los resultados en silver/_staging/, para que un reintento no
   repita llamadas.
3. publish: incorpora el lote al conjunto vigente y reescribe las
   particiones afectadas.

Cada tabla vive en un archivo Parquet por partición, por ejemplo:
silver/news/published_date=AAAA-MM-DD/news.parquet

Reescribir un objeto en S3 es atómico, por lo que Athena ve la versión
anterior o la nueva de cada archivo, nunca uno a medio escribir.
"""

import argparse
import hashlib
import html
import io
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urlsplit, urlunsplit

import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError
from lxml import etree
from lxml import html as lxml_html

from src.pipeline.enrichment import FINAL_STATUSES, analyze_texts
from src.pipeline.ingestion import (
    BUCKET,
    _is_not_found,
    batch_status,
    find_manifest_key,
    get_object,
    s3,
    utc_now,
    validate_xml,
)


NEWS_PREFIX = "silver/news/"
NEWS_FILE = "news.parquet"
ENTITIES_PREFIX = "silver/news_entities/"
ENTITIES_FILE = "news_entities.parquet"
KEY_PHRASES_PREFIX = "silver/news_key_phrases/"
KEY_PHRASES_FILE = "news_key_phrases.parquet"
QUARANTINE_PREFIX = "quarantine/news/"
BATCHES_PREFIX = "silver/_batches/"
STAGING_PREFIX = "silver/_staging/"

UTC_TIMESTAMP = pa.timestamp("us", tz="UTC")

# published_date no se guarda dentro de los archivos: es la clave de
# partición de las tres tablas.
NEWS_SCHEMA = pa.schema([
    ("news_id", pa.string()),
    ("guid", pa.string()),
    ("link", pa.string()),
    ("title", pa.string()),
    ("description", pa.string()),
    ("medium", pa.string()),
    ("sections", pa.list_(pa.string())),
    ("source_ids", pa.list_(pa.string())),
    ("pub_date_raw", pa.string()),
    ("published_at", UTC_TIMESTAMP),
    ("content_hash", pa.string()),
    ("version", pa.int32()),
    ("first_seen_at", UTC_TIMESTAMP),
    ("updated_at", UTC_TIMESTAMP),
    ("first_run_id", pa.string()),
    ("last_run_id", pa.string()),
    ("bronze_key", pa.string()),
    ("processed_at", UTC_TIMESTAMP),
    ("analysis_status", pa.string()),
    ("analysis_language", pa.string()),
    ("analysis_language_score", pa.float64()),
    ("analysis_error", pa.string()),
    ("analyzed_text", pa.string()),
    ("analyzed_text_hash", pa.string()),
    ("analyzed_at", UTC_TIMESTAMP),
])

ENTITIES_SCHEMA = pa.schema([
    ("news_id", pa.string()),
    ("entity_text", pa.string()),
    ("entity_text_normalized", pa.string()),
    ("entity_type", pa.string()),
    ("score", pa.float64()),
    ("mention_count", pa.int32()),
    ("text_hash", pa.string()),
    ("language", pa.string()),
    ("analyzed_at", UTC_TIMESTAMP),
])

KEY_PHRASES_SCHEMA = pa.schema([
    ("news_id", pa.string()),
    ("key_phrase", pa.string()),
    ("key_phrase_normalized", pa.string()),
    ("score", pa.float64()),
    ("mention_count", pa.int32()),
    ("text_hash", pa.string()),
    ("language", pa.string()),
    ("analyzed_at", UTC_TIMESTAMP),
])

WHITESPACE = re.compile(r"\s+")


def clean_text(value):
    """Quita etiquetas y entidades HTML; devuelve None si queda vacío."""
    if value is None:
        return None

    text = html.unescape(value)

    if "<" in text:
        text = lxml_html.fragment_fromstring(
            text,
            create_parent="div",
        ).text_content()

    text = WHITESPACE.sub(" ", text).strip()
    return text or None


def parse_pub_date(raw):
    """Interpreta fechas RFC 822 o ISO 8601 y las convierte a UTC."""
    if not raw or not raw.strip():
        return None

    raw = raw.strip()

    try:
        parsed = parsedate_to_datetime(raw)
    except (TypeError, ValueError, IndexError):
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            return None

    # Sin zona horaria no se puede saber el instante real.
    if parsed.tzinfo is None:
        return None

    return parsed.astimezone(timezone.utc)


def normalize_link(raw):
    """Valida un enlace http(s) y normaliza esquema, dominio y fragmento."""
    if not raw or not raw.strip():
        return None

    try:
        parts = urlsplit(raw.strip())
    except ValueError:
        return None

    scheme = parts.scheme.lower()

    if scheme not in ("http", "https") or not parts.netloc:
        return None

    return urlunsplit(
        (scheme, parts.netloc.lower(), parts.path, parts.query, "")
    )


def make_news_id(guid, link):
    """Identificador estable: hash del GUID o, si no existe, del enlace."""
    basis = guid if guid else link
    return hashlib.sha256(basis.encode("utf-8")).hexdigest()


def analysis_text(title, description):
    """Texto que se envía a Comprehend: título y descripción."""
    return f"{title}\n{description or ''}"


def content_hash(title, description):
    """Hash del texto que se analiza; detecta cambios en una noticia."""
    text = analysis_text(title, description)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _child_text(item, name):
    """Texto del primer hijo con ese nombre, sin importar el namespace."""
    for child in item:
        if (
            isinstance(child.tag, str)
            and etree.QName(child).localname == name
        ):
            return "".join(child.itertext())

    return None


def parse_items(content):
    """Extrae los campos crudos de cada <item> del RSS."""
    root, _ = validate_xml(content)

    return [
        {
            "item_index": index,
            "guid": _child_text(item, "guid"),
            "title": _child_text(item, "title"),
            "link": _child_text(item, "link"),
            "description": _child_text(item, "description"),
            "pub_date_raw": _child_text(item, "pubDate"),
        }
        for index, item in enumerate(
            root.xpath("//*[local-name()='item']")
        )
    ]


def validate_item(raw, source, run_id, fetched_at):
    """
    Limpia y valida un item.

    Devuelve (registro, []) si es válido o (None, motivos) si se rechaza.
    """
    reasons = []

    title = clean_text(raw["title"])
    description = clean_text(raw["description"])
    link = normalize_link(raw["link"])
    published_at = parse_pub_date(raw["pub_date_raw"])
    guid = (raw["guid"] or "").strip() or None

    if not title:
        reasons.append("titulo_vacio")

    if not (raw["link"] or "").strip():
        reasons.append("enlace_vacio")
    elif link is None:
        reasons.append("enlace_invalido")

    if not (raw["pub_date_raw"] or "").strip():
        reasons.append("fecha_vacia")
    elif published_at is None:
        reasons.append("fecha_invalida")

    if reasons:
        return None, reasons

    return {
        "news_id": make_news_id(guid, link),
        "guid": guid,
        "link": link,
        "title": title,
        "description": description,
        "medium": source["medium"],
        "section": source["section"],
        "source_id": source["source_id"],
        "pub_date_raw": raw["pub_date_raw"].strip(),
        "published_at": published_at,
        "content_hash": content_hash(title, description),
        "bronze_key": source["s3_key"],
        "run_id": run_id,
        "fetched_at": fetched_at,
    }, []


def collapse_batch(records):
    """
    Deja una aparición por noticia dentro del lote.

    Una noticia publicada en varios feeds conserva todas sus secciones.
    Devuelve los registros únicos y la cantidad de duplicados internos.
    """
    groups = defaultdict(list)

    for record in records:
        groups[record["news_id"]].append(record)

    collapsed = []

    for news_id in sorted(groups):
        group = groups[news_id]
        chosen = max(
            group,
            key=lambda item: (item["fetched_at"], item["source_id"]),
        )
        collapsed.append({
            **chosen,
            "sections": sorted({item["section"] for item in group}),
            "source_ids": sorted({item["source_id"] for item in group}),
        })

    return collapsed, len(records) - len(collapsed)


def partition_date(row):
    """Fecha de publicación en UTC usada como partición."""
    return row["published_at"].date().isoformat()


def _content_fields(record):
    """Campos de una noticia que provienen del registro entrante."""
    return {
        "guid": record["guid"],
        "link": record["link"],
        "title": record["title"],
        "description": record["description"],
        "medium": record["medium"],
        "pub_date_raw": record["pub_date_raw"],
        "published_at": record["published_at"],
        "content_hash": record["content_hash"],
        "bronze_key": record["bronze_key"],
        "last_run_id": record["run_id"],
        "updated_at": record["fetched_at"],
    }


def merge_batch(state, batch, processed_at):
    """
    Incorpora un lote al conjunto vigente (modifica state).

    Clasifica cada noticia como new, updated, repeated o stale.
    stale significa que el lote es más antiguo que la versión vigente;
    así, reprocesar un lote viejo no revierte una actualización.

    Devuelve (conteos, particiones modificadas, particiones que reciben
    noticias nuevas o actualizadas).
    """
    stats = Counter()
    dirty = set()
    gaining = set()

    for record in batch:
        news_id = record["news_id"]
        current = state.get(news_id)

        if current is None:
            state[news_id] = {
                "news_id": news_id,
                **_content_fields(record),
                "sections": record["sections"],
                "source_ids": record["source_ids"],
                "version": 1,
                "first_seen_at": record["fetched_at"],
                "first_run_id": record["run_id"],
                "processed_at": processed_at,
            }
            stats["new"] += 1
            dirty.add(partition_date(state[news_id]))
            gaining.add(partition_date(state[news_id]))
            continue

        sections = sorted(set(current["sections"]) | set(record["sections"]))
        source_ids = sorted(
            set(current["source_ids"]) | set(record["source_ids"])
        )

        if record["content_hash"] == current["content_hash"]:
            stats["repeated"] += 1

            # La misma noticia apareció en otra sección.
            if (
                sections != current["sections"]
                or source_ids != current["source_ids"]
            ):
                current.update(
                    sections=sections,
                    source_ids=source_ids,
                    processed_at=processed_at,
                )
                dirty.add(partition_date(current))

            continue

        if record["fetched_at"] < current["updated_at"]:
            stats["stale"] += 1
            continue

        old_date = partition_date(current)
        state[news_id] = {
            **current,
            **_content_fields(record),
            "sections": sections,
            "source_ids": source_ids,
            "version": current["version"] + 1,
            "processed_at": processed_at,
        }
        stats["updated"] += 1
        dirty.update({old_date, partition_date(state[news_id])})
        gaining.add(partition_date(state[news_id]))

    return stats, dirty, gaining


def needs_analysis(row):
    """Una noticia requiere análisis si su texto vigente no tiene uno final."""
    return (
        row.get("analyzed_text_hash") != row["content_hash"]
        or row.get("analysis_status") not in FINAL_STATUSES
    )


def enrichment_candidates(state):
    """Textos distintos que deben enviarse a Comprehend: {hash: texto}."""
    return {
        row["content_hash"]: analysis_text(row["title"], row["description"])
        for row in state.values()
        if needs_analysis(row)
    }


def apply_analysis(row, result):
    """Asigna a una noticia el resultado del análisis de su texto."""
    row.update(
        analysis_status=result["status"],
        analysis_language=result["language"],
        analysis_language_score=result["language_score"],
        analysis_error=result["error"],
        analyzed_text=result["text"],
        analyzed_text_hash=row["content_hash"],
        analyzed_at=result["analyzed_at"],
        entities=result["entities"],
        key_phrases=result["key_phrases"],
    )


def mark_pending(row):
    """
    Deja una noticia sin análisis vigente.

    Ocurre si su texto cambió y no hay resultado disponible; las entidades
    del texto anterior se eliminan para no atribuirlas al nuevo.
    Devuelve True si la noticia cambió.
    """
    if (
        row.get("analysis_status") == "pending"
        and not row.get("entities")
        and not row.get("key_phrases")
    ):
        return False

    row.update(
        analysis_status="pending",
        analysis_language=None,
        analysis_language_score=None,
        analysis_error=None,
        analyzed_text=None,
        analyzed_text_hash=None,
        analyzed_at=None,
        entities=[],
        key_phrases=[],
    )
    return True


def attach_analyses(state, analyses):
    """
    Aplica los resultados de Comprehend al conjunto vigente (modifica state).

    Devuelve las particiones modificadas y los conteos por resultado.
    """
    dirty = set()
    stats = Counter()

    for row in state.values():
        if not needs_analysis(row):
            continue

        result = analyses.get(row["content_hash"])

        if result is not None:
            apply_analysis(row, result)
            stats[f"applied_{result['status']}"] += 1
            dirty.add(partition_date(row))
        elif mark_pending(row):
            stats["pending"] += 1
            dirty.add(partition_date(row))

    return dirty, stats


def _partition_from_key(key):
    """Extrae AAAA-MM-DD de .../published_date=AAAA-MM-DD/archivo.parquet."""
    return key.split("published_date=", 1)[1].split("/", 1)[0]


def _read_partitions(prefix, file_name):
    """Recorre (fecha de partición, filas) de una tabla de Silver."""
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]

            if not key.endswith(f"/{file_name}"):
                continue

            table = pq.read_table(io.BytesIO(get_object(key)))
            yield _partition_from_key(key), table.to_pylist()


def _entity_from_row(row):
    return {
        "text": row["entity_text"],
        "normalized": row["entity_text_normalized"],
        "type": row["entity_type"],
        "score": row["score"],
        "mention_count": row["mention_count"],
    }


def _phrase_from_row(row):
    return {
        "text": row["key_phrase"],
        "normalized": row["key_phrase_normalized"],
        "score": row["score"],
        "mention_count": row["mention_count"],
    }


def _load_children(state, dirty, prefix, file_name, field, convert):
    """
    Asocia entidades o frases clave a sus noticias.

    Solo se conservan las del texto analizado vigente; las huérfanas o
    ubicadas en otra partición marcan la partición para reescribirla.
    """
    seen = defaultdict(set)

    for row in state.values():
        row[field] = []

    for file_date, rows in _read_partitions(prefix, file_name):
        for child in rows:
            news = state.get(child["news_id"])

            if (
                news is None
                or partition_date(news) != file_date
                or child["text_hash"] != news.get("analyzed_text_hash")
            ):
                dirty.add(file_date)
                continue

            item = convert(child)
            key = (item.get("type"), item["normalized"])

            if key in seen[child["news_id"]]:
                dirty.add(file_date)
                continue

            seen[child["news_id"]].add(key)
            news[field].append(item)


def load_state():
    """
    Lee el conjunto vigente de Silver con sus entidades y frases clave.

    Si una escritura anterior quedó a medias y una noticia aparece en dos
    particiones, conserva la versión más reciente y marca ambas para
    reescribirlas.
    """
    state = {}
    dirty = set()

    for file_date, rows in _read_partitions(NEWS_PREFIX, NEWS_FILE):
        for row in rows:
            if partition_date(row) != file_date:
                dirty.update({file_date, partition_date(row)})

            existing = state.get(row["news_id"])

            if existing is None:
                state[row["news_id"]] = row
                continue

            dirty.update({file_date, partition_date(existing)})

            if (row["version"], row["updated_at"]) > (
                existing["version"],
                existing["updated_at"],
            ):
                state[row["news_id"]] = row

    _load_children(
        state, dirty, ENTITIES_PREFIX, ENTITIES_FILE,
        "entities", _entity_from_row,
    )
    _load_children(
        state, dirty, KEY_PHRASES_PREFIX, KEY_PHRASES_FILE,
        "key_phrases", _phrase_from_row,
    )

    return state, dirty


def _entity_rows(news):
    return [
        {
            "news_id": news["news_id"],
            "entity_text": item["text"],
            "entity_text_normalized": item["normalized"],
            "entity_type": item["type"],
            "score": item["score"],
            "mention_count": item["mention_count"],
            "text_hash": news["analyzed_text_hash"],
            "language": news["analysis_language"],
            "analyzed_at": news["analyzed_at"],
        }
        for item in news.get("entities", [])
    ]


def _phrase_rows(news):
    return [
        {
            "news_id": news["news_id"],
            "key_phrase": item["text"],
            "key_phrase_normalized": item["normalized"],
            "score": item["score"],
            "mention_count": item["mention_count"],
            "text_hash": news["analyzed_text_hash"],
            "language": news["analysis_language"],
            "analyzed_at": news["analyzed_at"],
        }
        for item in news.get("key_phrases", [])
    ]


TABLES = [
    (NEWS_PREFIX, NEWS_FILE, NEWS_SCHEMA, lambda news: [news]),
    (ENTITIES_PREFIX, ENTITIES_FILE, ENTITIES_SCHEMA, _entity_rows),
    (KEY_PHRASES_PREFIX, KEY_PHRASES_FILE, KEY_PHRASES_SCHEMA, _phrase_rows),
]


def _write_table(key, rows, schema):
    """Reemplaza un archivo de partición, o lo elimina si queda vacío."""
    if not rows:
        s3.delete_object(Bucket=BUCKET, Key=key)
        return False

    buffer = io.BytesIO()
    pq.write_table(
        pa.Table.from_pylist(rows, schema=schema),
        buffer,
        compression="snappy",
    )
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=buffer.getvalue(),
        ContentType="application/vnd.apache.parquet",
    )
    return True


def write_partitions(state, dirty, gaining):
    """
    Reescribe las particiones modificadas de las tres tablas.

    Primero escribe las que reciben noticias y después las que solo
    pierden: si el proceso se interrumpe, una noticia puede quedar
    duplicada temporalmente (load_state lo corrige), pero nunca perdida.
    """
    news_by_date = defaultdict(list)

    for row in state.values():
        if partition_date(row) in dirty:
            news_by_date[partition_date(row)].append(row)

    written = []

    for day in sorted(dirty, key=lambda value: (value not in gaining, value)):
        news = sorted(news_by_date[day], key=lambda row: row["news_id"])

        for prefix, file_name, schema, rows_of in TABLES:
            rows = [row for item in news for row in rows_of(item)]
            _write_table(
                f"{prefix}published_date={day}/{file_name}",
                rows,
                schema,
            )

        if news:
            written.append(day)

    return written


def _put_json(key, body):
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )


def _json_default(value):
    if isinstance(value, datetime):
        return value.isoformat()

    raise TypeError(f"No se puede serializar {type(value).__name__}")


def _put_staging(run_id, name, payload):
    _put_json(
        f"{STAGING_PREFIX}run_id={run_id}/{name}",
        json.dumps(payload, ensure_ascii=False, default=_json_default),
    )


def _get_staging(run_id, name):
    """Lee un archivo de staging del lote; None si no existe."""
    try:
        body = get_object(f"{STAGING_PREFIX}run_id={run_id}/{name}")
    except ClientError as exc:
        if _is_not_found(exc):
            return None
        raise

    return json.loads(body)


def _load_batch(run_id):
    staged = _get_staging(run_id, "batch.json")

    if staged is None:
        raise RuntimeError(
            f"No existe el lote preparado de {run_id}; "
            "ejecuta primero prepare_silver."
        )

    for record in staged["records"]:
        record["published_at"] = datetime.fromisoformat(record["published_at"])
        record["fetched_at"] = datetime.fromisoformat(record["fetched_at"])

    return staged


def _load_analyses(run_id):
    stored = _get_staging(run_id, "enrichment.json") or {"results": {}}
    results = stored["results"]

    for result in results.values():
        result["analyzed_at"] = datetime.fromisoformat(result["analyzed_at"])

    return stored


def prepare_silver(run_id, ingestion_date=None):
    """
    Etapa 1: limpia, valida y deduplica un lote de Bronze.

    No consulta los RSS ni modifica las tablas de Silver.
    """
    manifest_key = find_manifest_key(run_id, ingestion_date)
    manifest = json.loads(get_object(manifest_key))

    if manifest.get("run_id") != run_id:
        raise ValueError("El run_id del manifiesto no coincide.")

    ingestion_date = manifest["ingestion_date"]
    bronze_status = manifest.get("status") or batch_status(manifest)

    if bronze_status == "partial":
        print(f"PARCIAL | {run_id} | se procesan solo las fuentes exitosas")

    valid = []
    rejected = []
    processed_sources = []
    skipped_sources = []

    for source in manifest.get("sources", []):
        source_id = source.get("source_id", "desconocida")

        if source.get("status") != "success":
            skipped_sources.append({
                "source_id": source_id,
                "error": source.get("error"),
            })
            print(f"OMITIDA | {run_id} | {source_id} | captura fallida")
            continue

        content = get_object(source["s3_key"])

        if hashlib.sha256(content).hexdigest() != source["sha256"]:
            raise ValueError(
                f"Hash incorrecto para s3://{BUCKET}/{source['s3_key']}"
            )

        fetched_at = datetime.fromisoformat(source["fetched_at"])
        items = parse_items(content)
        source_rejected = 0

        for raw in items:
            record, reasons = validate_item(
                raw,
                source,
                run_id,
                fetched_at,
            )

            if record is not None:
                valid.append(record)
                continue

            source_rejected += 1
            rejected.append({
                "run_id": run_id,
                "ingestion_date": ingestion_date,
                "source_id": source_id,
                "bronze_key": source["s3_key"],
                "item_index": raw["item_index"],
                "reasons": reasons,
                "raw": {
                    field: raw[field]
                    for field in (
                        "guid",
                        "title",
                        "link",
                        "description",
                        "pub_date_raw",
                    )
                },
            })

        processed_sources.append(source_id)
        print(
            f"LEIDA | {run_id} | {source_id} | {len(items)} items | "
            f"{source_rejected} rechazados"
        )

    if not processed_sources:
        raise RuntimeError(
            f"El lote {run_id} no tiene capturas exitosas en Bronze."
        )

    batch, duplicates_in_batch = collapse_batch(valid)
    quarantine_key = None

    if rejected:
        quarantine_key = (
            f"{QUARANTINE_PREFIX}ingestion_date={ingestion_date}/"
            f"run_id={run_id}/rejected.jsonl"
        )
        _put_json(
            quarantine_key,
            "\n".join(
                json.dumps(item, ensure_ascii=False) for item in rejected
            ) + "\n",
        )

    counts = {
        "run_id": run_id,
        "ingestion_date": ingestion_date,
        "manifest_key": manifest_key,
        "bronze_status": bronze_status,
        "processed_sources": processed_sources,
        "skipped_sources": skipped_sources,
        "items_received": len(valid) + len(rejected),
        "items_rejected": len(rejected),
        "items_valid": len(valid),
        "duplicates_in_batch": duplicates_in_batch,
        "unique_news": len(batch),
        "quarantine_key": quarantine_key,
    }

    _put_staging(run_id, "batch.json", {**counts, "records": batch})

    print(
        f"PREPARADO | {run_id} | recibidos={counts['items_received']} "
        f"rechazados={counts['items_rejected']} "
        f"duplicados_lote={duplicates_in_batch} unicas={len(batch)}"
    )

    return counts


def enrich_silver(run_id):
    """
    Etapa 2: analiza con Comprehend los textos que aún no tienen análisis.

    Simula la incorporación del lote para saber qué textos quedarán
    vigentes. Los resultados se guardan en staging: si la tarea se
    reintenta, solo se envían los textos que faltan.
    """
    staged = _load_batch(run_id)
    state, _ = load_state()
    merge_batch(state, staged["records"], utc_now())

    candidates = enrichment_candidates(state)
    stored = _load_analyses(run_id)
    results = stored["results"]
    missing = {
        text_hash: text
        for text_hash, text in candidates.items()
        if text_hash not in results
    }

    new_results, stats = analyze_texts(missing) if missing else ({}, {})
    results.update(new_results)

    calls = Counter(stored.get("api_calls", {}))
    calls.update(stats.get("api_calls", {}))

    _put_staging(run_id, "enrichment.json", {
        "results": results,
        "api_calls": dict(calls),
    })

    summary = {
        "texts_pending": len(candidates),
        "texts_reused_from_retry": len(candidates) - len(missing),
        "texts_sent": len(missing),
        "api_calls": stats.get("api_calls", {}),
        "api_calls_batch_total": dict(calls),
        "statuses": dict(
            Counter(results[text_hash]["status"] for text_hash in candidates)
        ),
    }

    print(
        f"COMPREHEND | {run_id} | pendientes={summary['texts_pending']} "
        f"enviados={summary['texts_sent']} "
        f"reutilizados={summary['texts_reused_from_retry']} "
        f"llamadas={summary['api_calls']} estados={summary['statuses']}"
    )

    return summary


def publish_silver(run_id):
    """
    Etapa 3: incorpora el lote y los análisis al conjunto vigente.

    Ejecutarla varias veces sobre el mismo lote produce el mismo resultado.
    """
    staged = _load_batch(run_id)
    analyses = _load_analyses(run_id)
    processed_at = utc_now()

    state, dirty = load_state()
    stats, changed, gaining = merge_batch(
        state,
        staged["records"],
        processed_at,
    )
    analysis_dirty, analysis_stats = attach_analyses(
        state,
        analyses["results"],
    )
    dirty |= changed | analysis_dirty
    written = write_partitions(state, dirty, gaining)

    summary = {
        **{key: value for key, value in staged.items() if key != "records"},
        "processed_at": processed_at.isoformat(),
        "new": stats["new"],
        "updated": stats["updated"],
        "repeated": stats["repeated"],
        "stale": stats["stale"],
        "analysis": dict(analysis_stats),
        "comprehend_api_calls_batch_total": analyses.get("api_calls", {}),
        "partitions_written": written,
        "current_news_total": len(state),
        "current_analysis_status": dict(
            Counter(row["analysis_status"] for row in state.values())
        ),
        "current_entities_total": sum(
            len(row["entities"]) for row in state.values()
        ),
        "current_key_phrases_total": sum(
            len(row["key_phrases"]) for row in state.values()
        ),
    }

    _put_json(
        f"{BATCHES_PREFIX}ingestion_date={staged['ingestion_date']}/"
        f"run_id={run_id}/summary.json",
        json.dumps(summary, ensure_ascii=False, indent=2),
    )

    print(
        f"SILVER | {run_id} | nuevas={stats['new']} "
        f"actualizadas={stats['updated']} repetidas={stats['repeated']} "
        f"antiguas={stats['stale']} analisis={dict(analysis_stats)} "
        f"particiones={written}"
    )

    return summary


def build_silver(run_id, ingestion_date=None):
    """Ejecuta las tres etapas de Silver para un lote de Bronze."""
    prepare_silver(run_id, ingestion_date)
    enrich_silver(run_id)
    return publish_silver(run_id)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Construye Silver a partir de un lote de Bronze."
    )
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--ingestion-date")
    args = parser.parse_args()

    build_silver(args.run_id, args.ingestion_date)
