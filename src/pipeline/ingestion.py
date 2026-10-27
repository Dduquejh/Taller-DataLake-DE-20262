
import argparse
import hashlib
import json
import os
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


def extract(run_id=None, ingestion_date=None):
    """
    Captura fuentes RSS y conserva los archivos originales en Bronze.

    run_id e ingestion_date pueden proporcionarse desde Airflow.
    Si ya existe el manifiesto de ese lote, no vuelve a consultar los RSS.
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

    # Si el lote ya terminó, devolverlo sin volver a consultar los RSS.
    try:
        existing = s3.get_object(Bucket=BUCKET, Key=manifest_key)
    except ClientError as exc:
        status = exc.response.get("ResponseMetadata", {}).get(
            "HTTPStatusCode"
        )
        code = exc.response.get("Error", {}).get("Code")

        if status != 404 and code not in (
            "NoSuchKey",
            "404",
            "NotFound",
        ):
            raise
    else:
        manifest = json.loads(existing["Body"].read())

        if manifest.get("run_id") != run_id:
            raise ValueError("El run_id del manifiesto no coincide.")

        if manifest.get("ingestion_date") != ingestion_date:
            raise ValueError(
                "La fecha del manifiesto no coincide con ingestion_date."
            )

        if manifest.get("error_count", 0) > 0:
            raise RuntimeError(
                f"El lote {run_id} ya existe, pero está incompleto. "
                "Usa el DAG de reprocesamiento para verificar "
                "las capturas exitosas."
            )

        print(f"Lote existente: {run_id}")
        print(f"MANIFEST_KEY={manifest_key}")
        return run_id

    manifest = {
        "run_id": run_id,
        "ingestion_date": ingestion_date,
        "started_at": started_at.isoformat(),
        "bucket": BUCKET,
        "region": REGION,
        "operation": "extract",
        "sources": [],
    }

    for source in load_sources():
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
                response = requests.get(
                    source["url"],
                    timeout=30,
                    headers={
                        "User-Agent": "NoticiasDataLake/1.0"
                    },
                )

                result["http_status"] = response.status_code
                result["content_type"] = response.headers.get(
                    "Content-Type"
                )
                response.raise_for_status()

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
                    "reused_existing_object": (
                        original != response.content
                    ),
                })

        except Exception as exc:
            result["error"] = f"{type(exc).__name__}: {exc}"
            print(f"ERROR | {source_id} | {result['error']}")

        manifest["sources"].append(result)

        print(
            f"{result['status'].upper()} | {source_id} | "
            f"HTTP {result.get('http_status', 'N/A')} | "
            f"{result.get('news_count', 0)} noticias"
        )

    manifest["finished_at"] = utc_now().isoformat()
    manifest["success_count"] = sum(
        item["status"] == "success"
        for item in manifest["sources"]
    )
    manifest["error_count"] = sum(
        item["status"] == "error"
        for item in manifest["sources"]
    )

    # El manifiesto registra el resultado del lote.
    s3.put_object(
        Bucket=BUCKET,
        Key=manifest_key,
        Body=json.dumps(
            manifest,
            ensure_ascii=False,
            indent=2,
        ).encode("utf-8"),
        ContentType="application/json",
        IfNoneMatch="*",
    )

    print(f"\nRUN_ID={run_id}")
    print(f"MANIFEST_KEY={manifest_key}")
    print(
        f"Resultado: {manifest['success_count']} correctas, "
        f"{manifest['error_count']} fallidas"
    )

    if manifest["error_count"]:
        raise RuntimeError(
            f"Lote {run_id} incompleto. Revisa su manifiesto."
        )

    return run_id


def reprocess(run_id):
    """
    Verifica un lote existente sin consultar fuentes RSS
    ni escribir objetos en S3.
    """
    if (
        not isinstance(run_id, str)
        or not run_id.strip()
        or any(char in run_id for char in ("/", "\\"))
    ):
        raise ValueError("run_id inválido.")

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

    manifest_key = matches[0]
    manifest = json.loads(get_object(manifest_key))

    if manifest.get("run_id") != run_id:
        raise ValueError("El run_id del manifiesto no coincide.")

    print(f"Reprocesando lote existente: {run_id}")
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
