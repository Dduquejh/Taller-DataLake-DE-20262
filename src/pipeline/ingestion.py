
import argparse
import hashlib
import json
import os
import time
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

import boto3
import requests
import yaml
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from lxml import etree


ROOT_DIR = Path(__file__).resolve().parents[2]
load_dotenv(ROOT_DIR / ".env")

SOURCES_FILE = ROOT_DIR / "config" / "sources.yaml"
BUCKET = os.environ["S3_BUCKET"]
REGION = os.getenv("AWS_REGION", "us-east-2")

FETCH_ATTEMPTS = 3
RETRY_BASE_SECONDS = 2
RETRY_HTTP_STATUS = {429, 500, 502, 503, 504}

s3 = boto3.Session(region_name=REGION).client("s3")


def utc_now():
    """Devuelve la fecha y hora actual en UTC."""
    return datetime.now(timezone.utc)


def load_sources():
    """Carga y valida las fuentes RSS configuradas."""
    with SOURCES_FILE.open(encoding="utf-8") as file:
        config = yaml.safe_load(file) or {}

    sources = config.get("sources", [])

    if not sources:
        raise ValueError("No hay fuentes configuradas en sources.yaml.")

    required = {"source_id", "medium", "section", "url"}

    for source in sources:
        missing = required - source.keys()
        if missing:
            raise ValueError(
                f"Fuente inválida; faltan campos: {sorted(missing)}"
            )

    return sources


def get_object(key):
    """Lee un objeto existente de S3."""
    response = s3.get_object(Bucket=BUCKET, Key=key)
    return response["Body"].read()


def object_exists(key):
    """Comprueba si existe un objeto en S3."""
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
        return True
    except ClientError as exc:
        status = exc.response.get("ResponseMetadata", {}).get(
            "HTTPStatusCode"
        )
        code = exc.response.get("Error", {}).get("Code")

        if status == 404 or code in ("404", "NoSuchKey", "NotFound"):
            return False

        raise


def validate_xml(content):
    """Valida el XML y devuelve su raíz y cantidad de noticias."""
    parser = etree.XMLParser(
        resolve_entities=False,
        no_network=True,
        recover=False,
    )
    root = etree.fromstring(content, parser=parser)
    news_count = len(root.xpath("//*[local-name()='item']"))
    return root, news_count


def _is_not_found(exc):
    """Indica si un ClientError de S3 corresponde a un objeto inexistente."""
    status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    code = exc.response.get("Error", {}).get("Code")
    return status == 404 or code in ("NoSuchKey", "404", "NotFound")


def batch_status(manifest):
    """
    Estado del lote según sus fuentes.

    complete: todas las fuentes se capturaron.
    partial: al menos una se capturó y al menos una falló.
    failed: ninguna se capturó.
    """
    sources = manifest.get("sources", [])
    success = sum(item.get("status") == "success" for item in sources)

    if sources and success == len(sources):
        return "complete"

    if success:
        return "partial"

    return "failed"


def fetch_feed(url):
    """
    Descarga un feed.

    Reintenta errores de red y respuestas 429/5xx; los demás estados
    HTTP se devuelven de inmediato porque repetir no los corrige.
    """
    for attempt in range(1, FETCH_ATTEMPTS + 1):
        try:
            response = requests.get(
                url,
                timeout=30,
                headers={"User-Agent": "NoticiasDataLake/1.0"},
            )
        except (requests.ConnectionError, requests.Timeout) as exc:
            if attempt == FETCH_ATTEMPTS:
                raise

            print(f"REINTENTO | {url} | intento {attempt} | {exc}")
        else:
            if (
                response.status_code not in RETRY_HTTP_STATUS
                or attempt == FETCH_ATTEMPTS
            ):
                return response

            print(
                f"REINTENTO | {url} | intento {attempt} | "
                f"HTTP {response.status_code}"
            )

        time.sleep(RETRY_BASE_SECONDS * 2 ** (attempt - 1))


def capture_source(source, run_id, ingestion_date):
    """Captura una fuente en Bronze y devuelve su registro del manifiesto."""
    source_id = source["source_id"]

    result = {
        "source_id": source_id,
        "medium": source["medium"],
        "section": source["section"],
        "url": source["url"],
        "fetched_at": utc_now().isoformat(),
        "status": "error",
    }

    key = (
        f"bronze/source={source_id}/"
        f"ingestion_date={ingestion_date}/"
        f"run_id={run_id}/response.xml"
    )

    try:
        # Si el archivo ya existe, conservar y validar el original.
        # Esto evita sobrescribir una captura durante un reintento.
        if object_exists(key):
            original = get_object(key)
            _, news_count = validate_xml(original)
            digest = hashlib.sha256(original).hexdigest()

            result.update({
                "status": "success",
                "sha256": digest,
                "s3_key": key,
                "bytes": len(original),
                "news_count": news_count,
                "reused_existing_object": True,
            })

        else:
            response = fetch_feed(source["url"])

            result["http_status"] = response.status_code
            result["content_type"] = response.headers.get("Content-Type")
            response.raise_for_status()

            # Una respuesta vacía es un error, no una fuente sin noticias.
            if not response.content.strip():
                raise ValueError("respuesta vacía")

            _, news_count = validate_xml(response.content)
            digest = hashlib.sha256(response.content).hexdigest()

            # Creación condicional para impedir sobrescrituras.
            try:
                s3.put_object(
                    Bucket=BUCKET,
                    Key=key,
                    Body=response.content,
                    ContentType="application/xml",
                    Metadata={
                        "source-id": source_id,
                        "run-id": run_id,
                        "sha256": digest,
                    },
                    IfNoneMatch="*",
                )
                original = response.content

            except ClientError as exc:
                code = exc.response.get("Error", {}).get("Code")
                status = exc.response.get(
                    "ResponseMetadata", {}
                ).get("HTTPStatusCode")

                # Otro intento pudo crear el objeto simultáneamente.
                if code not in ("PreconditionFailed", "412") and (
                    status != 412
                ):
                    raise

                original = get_object(key)
                _, news_count = validate_xml(original)
                digest = hashlib.sha256(original).hexdigest()

            result.update({
                "status": "success",
                "sha256": digest,
                "s3_key": key,
                "bytes": len(original),
                "news_count": news_count,
                "reused_existing_object": original != response.content,
            })

    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
        print(f"ERROR | {run_id} | {source_id} | {result['error']}")

    print(
        f"{result['status'].upper()} | {run_id} | {source_id} | "
        f"HTTP {result.get('http_status', 'N/A')} | "
        f"{result.get('news_count', 0)} noticias"
    )

    return result


def extract(run_id=None, ingestion_date=None):
    """
    Captura fuentes RSS y conserva los archivos originales en Bronze.

    run_id e ingestion_date pueden proporcionarse desde Airflow.

    - Lote complete existente: se devuelve sin consultar los RSS.
    - Lote partial o failed existente: se reutilizan las capturas
      guardadas y solo se consultan las fuentes que fallaron.
    - Resultado failed: lanza una excepción para que Airflow reintente.
    - Resultado partial: se registra y continúa con las fuentes exitosas.
    """
    started_at = utc_now()

    if ingestion_date is None:
        ingestion_date = started_at.date().isoformat()

    try:
        ingestion_date = date.fromisoformat(ingestion_date).isoformat()
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "ingestion_date debe tener formato YYYY-MM-DD."
        ) from exc

    if run_id is None:
        run_id = (
            started_at.strftime("%Y%m%dT%H%M%SZ")
            + "-"
            + uuid.uuid4().hex[:8]
        )

    if (
        not isinstance(run_id, str)
        or not run_id.strip()
        or any(char in run_id for char in ("/", "\\"))
    ):
        raise ValueError(
            "run_id debe ser una cadena no vacía sin barras."
        )

    manifest_key = (
        f"bronze/manifests/ingestion_date={ingestion_date}/"
        f"run_id={run_id}/manifest.json"
    )

    attempt = 1
    previous_status = None
    previous_etag = None

    try:
        existing = s3.get_object(Bucket=BUCKET, Key=manifest_key)
    except ClientError as exc:
        if not _is_not_found(exc):
            raise
    else:
        manifest = json.loads(existing["Body"].read())

        if manifest.get("run_id") != run_id:
            raise ValueError("El run_id del manifiesto no coincide.")

        if manifest.get("ingestion_date") != ingestion_date:
            raise ValueError(
                "La fecha del manifiesto no coincide con ingestion_date."
            )

        # Los manifiestos anteriores a este campo se evalúan por sus fuentes.
        previous_status = manifest.get("status") or batch_status(manifest)

        if previous_status == "complete":
            print(f"Lote existente completo: {run_id}")
            print(f"MANIFEST_KEY={manifest_key}")
            return run_id

        attempt = manifest.get("attempt", 1) + 1
        previous_etag = existing["ETag"]
        print(
            f"Recuperando lote {run_id} ({previous_status}); "
            f"intento {attempt}"
        )

    manifest = {
        "run_id": run_id,
        "ingestion_date": ingestion_date,
        "started_at": started_at.isoformat(),
        "bucket": BUCKET,
        "region": REGION,
        "operation": "extract",
        "attempt": attempt,
        "previous_status": previous_status,
        "sources": [
            capture_source(source, run_id, ingestion_date)
            for source in load_sources()
        ],
    }

    manifest["finished_at"] = utc_now().isoformat()
    manifest["success_count"] = sum(
        item["status"] == "success"
        for item in manifest["sources"]
    )
    manifest["error_count"] = sum(
        item["status"] == "error"
        for item in manifest["sources"]
    )
    manifest["status"] = batch_status(manifest)

    # Un manifiesto nuevo no puede pisar otro; uno recuperado solo
    # reemplaza la versión leída. El versionado del bucket conserva
    # los intentos anteriores.
    condition = (
        {"IfMatch": previous_etag}
        if previous_etag
        else {"IfNoneMatch": "*"}
    )

    s3.put_object(
        Bucket=BUCKET,
        Key=manifest_key,
        Body=json.dumps(
            manifest,
            ensure_ascii=False,
            indent=2,
        ).encode("utf-8"),
        ContentType="application/json",
        **condition,
    )

    print(f"\nRUN_ID={run_id}")
    print(f"MANIFEST_KEY={manifest_key}")
    print(
        f"Resultado: {manifest['status'].upper()} | "
        f"{manifest['success_count']} correctas, "
        f"{manifest['error_count']} fallidas"
    )

    if manifest["status"] == "failed":
        raise RuntimeError(
            f"Lote {run_id} fallido: ninguna fuente se capturó. "
            "Revisa su manifiesto."
        )

    if manifest["status"] == "partial":
        failed = [
            item["source_id"]
            for item in manifest["sources"]
            if item["status"] != "success"
        ]
        print(
            f"PARCIAL | {run_id} | fuentes fallidas: {failed}. "
            "Reejecuta la tarea para volver a intentarlas."
        )

    return run_id


def find_manifest_key(run_id, ingestion_date=None):
    """
    Devuelve la clave del manifiesto de un lote.

    Si no se conoce ingestion_date, busca el run_id en todas las fechas.
    """
    if (
        not isinstance(run_id, str)
        or not run_id.strip()
        or any(char in run_id for char in ("/", "\\"))
    ):
        raise ValueError("run_id inválido.")

    if ingestion_date:
        return (
            f"bronze/manifests/ingestion_date={ingestion_date}/"
            f"run_id={run_id}/manifest.json"
        )

    paginator = s3.get_paginator("list_objects_v2")
    matches = []

    prefix = "bronze/manifests/"

    for page in paginator.paginate(
        Bucket=BUCKET,
        Prefix=prefix,
    ):
        for obj in page.get("Contents", []):
            key = obj["Key"]

            if key.endswith(f"run_id={run_id}/manifest.json"):
                matches.append(key)

    if len(matches) != 1:
        raise ValueError(
            f"Se esperaba un manifiesto para run_id={run_id}; "
            f"se encontraron {len(matches)}."
        )

    return matches[0]


def reprocess(run_id):
    """
    Verifica un lote existente sin consultar fuentes RSS
    ni escribir objetos en S3.
    """
    manifest_key = find_manifest_key(run_id)
    manifest = json.loads(get_object(manifest_key))

    if manifest.get("run_id") != run_id:
        raise ValueError("El run_id del manifiesto no coincide.")

    status = manifest.get("status") or batch_status(manifest)
    print(f"Reprocesando lote existente: {run_id} ({status})")
    print(f"Manifiesto: s3://{BUCKET}/{manifest_key}")

    verified = 0

    for item in manifest.get("sources", []):
        if item.get("status") != "success":
            print(
                f"OMITIDA | {item.get('source_id', 'desconocida')} | "
                "la captura original no fue exitosa"
            )
            continue

        key = item["s3_key"]
        original = get_object(key)
        actual_hash = hashlib.sha256(original).hexdigest()

        if actual_hash != item["sha256"]:
            raise ValueError(
                f"Hash incorrecto para s3://{BUCKET}/{key}"
            )

        _, news_count = validate_xml(original)

        print(
            f"OK | {item['source_id']} | "
            f"SHA-256 verificado | {news_count} noticias | {key}"
        )
        verified += 1

    if verified == 0:
        raise RuntimeError(
            "No se pudo verificar ninguna captura válida."
        )

    print(
        f"\nReprocesamiento verificado: {verified} archivo(s). "
        "No se consultaron los feeds ni se escribieron objetos."
    )

    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Pipeline de ingesta RSS a Bronze en S3."
    )
    subparsers = parser.add_subparsers(
        dest="command",
        required=True,
    )

    extract_parser = subparsers.add_parser(
        "extract",
        help="Consultar RSS y crear un lote en Bronze.",
    )
    extract_parser.add_argument("--run-id")
    extract_parser.add_argument("--ingestion-date")

    replay_parser = subparsers.add_parser(
        "reprocess",
        help="Verificar un lote ya almacenado en S3.",
    )
    replay_parser.add_argument("--run-id", required=True)

    args = parser.parse_args()

    if args.command == "extract":
        extract(
            run_id=args.run_id,
            ingestion_date=args.ingestion_date,
        )
    elif args.command == "reprocess":
        reprocess(args.run_id)
