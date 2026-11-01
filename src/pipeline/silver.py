"""
Capa Silver: convierte los RSS originales de Bronze en noticias normalizadas.

El conjunto vigente vive en un archivo Parquet por partición:
silver/news/published_date=AAAA-MM-DD/news.parquet

Reescribir un objeto en S3 es atómico, por lo que Athena ve la versión
anterior o la nueva de cada partición, nunca un archivo a medio escribir.
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
from lxml import etree
from lxml import html as lxml_html

from src.pipeline.ingestion import (
    BUCKET,
    batch_status,
    find_manifest_key,
    get_object,
    s3,
    utc_now,
    validate_xml,
)


NEWS_PREFIX = "silver/news/"
NEWS_FILE = "news.parquet"
QUARANTINE_PREFIX = "quarantine/news/"
BATCHES_PREFIX = "silver/_batches/"

UTC_TIMESTAMP = pa.timestamp("us", tz="UTC")

# published_date no se guarda dentro del archivo: es la clave de partición.
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


def content_hash(title, description):
    """Hash del texto que se analiza; detecta cambios en una noticia."""
    text = f"{title}\n{description or ''}"
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


def _partition_from_key(key):
    """Extrae AAAA-MM-DD de .../published_date=AAAA-MM-DD/news.parquet."""
    return key.split("published_date=", 1)[1].split("/", 1)[0]


def load_state():
    """
    Lee el conjunto vigente de Silver.

    Si una escritura anterior quedó a medias y una noticia aparece en dos
    particiones, conserva la versión más reciente y marca ambas para
    reescribirlas.
    """
    state = {}
    dirty = set()
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=BUCKET, Prefix=NEWS_PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]

            if not key.endswith(f"/{NEWS_FILE}"):
                continue

            file_date = _partition_from_key(key)
            table = pq.read_table(io.BytesIO(get_object(key)))

            for row in table.to_pylist():
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

    return state, dirty


def write_partitions(state, dirty, gaining):
    """
    Reescribe las particiones modificadas.

    Primero escribe las que reciben noticias y después las que solo
    pierden: si el proceso se interrumpe, una noticia puede quedar
    duplicada temporalmente (load_state lo corrige), pero nunca perdida.
    """
    rows_by_date = defaultdict(list)

    for row in state.values():
        if partition_date(row) in dirty:
            rows_by_date[partition_date(row)].append(row)

    written = []

    for day in sorted(dirty, key=lambda value: (value not in gaining, value)):
        key = f"{NEWS_PREFIX}published_date={day}/{NEWS_FILE}"
        rows = sorted(rows_by_date[day], key=lambda row: row["news_id"])

        if not rows:
            s3.delete_object(Bucket=BUCKET, Key=key)
            continue

        buffer = io.BytesIO()
        pq.write_table(
            pa.Table.from_pylist(rows, schema=NEWS_SCHEMA),
            buffer,
            compression="snappy",
        )
        s3.put_object(
            Bucket=BUCKET,
            Key=key,
            Body=buffer.getvalue(),
            ContentType="application/vnd.apache.parquet",
        )
        written.append(day)

    return written


def _put_json(key, body):
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )


def build_silver(run_id, ingestion_date=None):
    """
    Construye Silver a partir de un lote de Bronze sin consultar los RSS.

    Ejecutarlo varias veces sobre el mismo lote produce el mismo resultado.
    """
    manifest_key = find_manifest_key(run_id, ingestion_date)
    manifest = json.loads(get_object(manifest_key))

    if manifest.get("run_id") != run_id:
        raise ValueError("El run_id del manifiesto no coincide.")

    ingestion_date = manifest["ingestion_date"]
    bronze_status = manifest.get("status") or batch_status(manifest)
    processed_at = utc_now()

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
    state, dirty = load_state()
    stats, changed, gaining = merge_batch(state, batch, processed_at)
    dirty |= changed
    written = write_partitions(state, dirty, gaining)

    batch_prefix = f"ingestion_date={ingestion_date}/run_id={run_id}"
    quarantine_key = None

    if rejected:
        quarantine_key = f"{QUARANTINE_PREFIX}{batch_prefix}/rejected.jsonl"
        _put_json(
            quarantine_key,
            "\n".join(
                json.dumps(item, ensure_ascii=False) for item in rejected
            ) + "\n",
        )

    summary = {
        "run_id": run_id,
        "ingestion_date": ingestion_date,
        "processed_at": processed_at.isoformat(),
        "manifest_key": manifest_key,
        "bronze_status": bronze_status,
        "processed_sources": processed_sources,
        "skipped_sources": skipped_sources,
        "items_received": len(valid) + len(rejected),
        "items_rejected": len(rejected),
        "items_valid": len(valid),
        "duplicates_in_batch": duplicates_in_batch,
        "unique_news": len(batch),
        "new": stats["new"],
        "updated": stats["updated"],
        "repeated": stats["repeated"],
        "stale": stats["stale"],
        "partitions_written": written,
        "current_news_total": len(state),
        "quarantine_key": quarantine_key,
    }

    _put_json(
        f"{BATCHES_PREFIX}{batch_prefix}/summary.json",
        json.dumps(summary, ensure_ascii=False, indent=2),
    )

    print(
        f"SILVER | {run_id} | recibidos={summary['items_received']} "
        f"rechazados={summary['items_rejected']} "
        f"duplicados_lote={duplicates_in_batch} "
        f"nuevas={stats['new']} actualizadas={stats['updated']} "
        f"repetidas={stats['repeated']} antiguas={stats['stale']} "
        f"particiones={written}"
    )

    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Construye Silver a partir de un lote de Bronze."
    )
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--ingestion-date")
    args = parser.parse_args()

    build_silver(args.run_id, args.ingestion_date)
