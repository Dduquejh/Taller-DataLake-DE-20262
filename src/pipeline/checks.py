"""
Tareas de control del DAG: verificación inicial, validaciones finales con
Athena y registro del estado final del procesamiento.

El estado final queda en gold/_runs/run_id=<run de Airflow>/status.json.
"""

import json

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from src.pipeline import athena as athena_queries
from src.pipeline.ingestion import (
    BUCKET,
    REGION,
    ROOT_DIR,
    find_manifest_key,
    get_object,
    load_sources,
    s3,
    utc_now,
)

VALIDATIONS_FILE = ROOT_DIR / "sql" / "06_validaciones_calidad.sql"
RUNS_PREFIX = "gold/_runs/"

# Tablas que crea terraform/catalog.
CATALOG_TABLES = [
    "news",
    "news_entities",
    "news_key_phrases",
    "news_daily",
    "entity_daily",
    "key_phrase_daily",
]

comprehend = boto3.Session(region_name=REGION).client("comprehend")
glue = boto3.Session(region_name=REGION).client("glue")


class ValidationFailed(RuntimeError):
    """Alguna regla de calidad tiene registros que la incumplen."""


class RunNotComplete(RuntimeError):
    """El procesamiento terminó, pero el lote no quedó completo."""


def _check(name, function, problems):
    try:
        detail = function()
        print(f"PREFLIGHT | OK    | {name} | {detail}")
    except (BotoCoreError, ClientError, OSError, ValueError, KeyError) as exc:
        problems.append(f"{name}: {exc}")
        print(f"PREFLIGHT | ERROR | {name} | {exc}")


def _check_s3():
    s3.head_bucket(Bucket=BUCKET)
    s3.list_objects_v2(Bucket=BUCKET, Prefix="bronze/manifests/", MaxKeys=1)
    return f"s3://{BUCKET}"


def _check_sources():
    return f"{len(load_sources())} fuentes en config/sources.yaml"


def _check_comprehend():
    # Una llamada mínima (3 unidades, una fracción de centavo) confirma
    # credenciales, región y permisos.
    response = comprehend.detect_dominant_language(Text="Prueba de acceso")
    return f"idioma detectado={response['Languages'][0]['LanguageCode']}"


def _check_glue():
    for table in CATALOG_TABLES:
        glue.get_table(DatabaseName=athena_queries.DATABASE, Name=table)
    return f"{athena_queries.DATABASE}: {len(CATALOG_TABLES)} tablas"


def _check_athena():
    athena_queries.athena.get_work_group(WorkGroup=athena_queries.WORKGROUP)
    return f"workgroup {athena_queries.WORKGROUP}"


def _check_validations_file():
    if not VALIDATIONS_FILE.is_file():
        raise ValueError(f"no existe {VALIDATIONS_FILE}")
    return VALIDATIONS_FILE.name


def preflight():
    """
    Verifica la configuración y el acceso a S3, Comprehend, Glue y Athena
    antes de consultar los RSS. Reporta todos los problemas juntos.
    """
    problems = []

    _check("configuración de fuentes", _check_sources, problems)
    _check("archivo de validaciones", _check_validations_file, problems)
    _check("S3", _check_s3, problems)
    _check("Comprehend", _check_comprehend, problems)
    _check("Glue Data Catalog", _check_glue, problems)
    _check("Athena", _check_athena, problems)

    if problems:
        raise RuntimeError(
            "Falló la verificación inicial:\n- " + "\n- ".join(problems)
        )

    return {"bucket": BUCKET, "region": REGION}


def validate_with_athena(run_id):
    """
    Ejecuta las reglas de sql/06_validaciones_calidad.sql en Athena y falla
    si alguna tiene registros que la incumplen.
    """
    sql = VALIDATIONS_FILE.read_text(encoding="utf-8")
    rows, scanned = athena_queries.query(sql)

    results = {row["check_name"]: int(row["failures"]) for row in rows}

    for name, failures in sorted(results.items()):
        mark = "OK   " if failures == 0 else "FALLA"
        print(f"VALIDACION | {run_id} | {mark} | {name} | {failures}")

    print(f"VALIDACION | {run_id} | {scanned} bytes escaneados")

    failing = {name: count for name, count in results.items() if count}

    if failing:
        raise ValidationFailed(
            f"{len(failing)} regla(s) de calidad con fallas: {failing}"
        )

    return {"checks": results, "bytes_scanned": scanned}


def _batch_status(batch_run_id, ingestion_date):
    """Lee el estado del lote desde su manifiesto de Bronze."""
    try:
        key = find_manifest_key(batch_run_id, ingestion_date)
        manifest = json.loads(get_object(key))
    except (ClientError, ValueError) as exc:
        return {"status": "unknown", "error": str(exc)}

    return {
        "status": manifest.get("status", "unknown"),
        "attempt": manifest.get("attempt"),
        "success_count": manifest.get("success_count"),
        "error_count": manifest.get("error_count"),
        "failed_sources": [
            item["source_id"]
            for item in manifest.get("sources", [])
            if item.get("status") != "success"
        ],
        "manifest_key": key,
    }


def final_status(batch, stages):
    """
    Combina el estado del lote y de las etapas en un estado final:

    - complete: el lote está completo y todas las etapas terminaron;
    - partial: todas las etapas terminaron, pero faltaron fuentes;
    - failed: alguna etapa no terminó o el lote falló.
    """
    if any(result is None for result in stages.values()):
        return "failed"

    if batch["status"] == "complete":
        return "complete"

    if batch["status"] == "partial":
        return "partial"

    return "failed"


def record_run_status(airflow_run_id, batch_run_id, ingestion_date, stages):
    """
    Guarda el estado final del procesamiento y falla si no es complete.

    stages: {tarea: resultado devuelto, o None si la tarea no terminó}.
    Corre aunque una tarea anterior haya fallado, para dejar registro; al
    fallar ella misma, la corrida de Airflow queda en rojo también en un
    lote parcial, que de otro modo se vería en verde.
    """
    batch = _batch_status(batch_run_id, ingestion_date)
    status = final_status(batch, stages)

    record = {
        "airflow_run_id": airflow_run_id,
        "batch_run_id": batch_run_id,
        "ingestion_date": ingestion_date,
        "recorded_at": utc_now().isoformat(),
        "status": status,
        "batch": batch,
        "stages": {
            task: "success" if result is not None else "not_completed"
            for task, result in stages.items()
        },
        "results": {
            task: result for task, result in stages.items() if result
        },
    }

    key = f"{RUNS_PREFIX}run_id={airflow_run_id}/status.json"
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(
            record, ensure_ascii=False, indent=2, default=str
        ).encode("utf-8"),
        ContentType="application/json",
    )

    pending = [task for task, result in stages.items() if result is None]
    print(
        f"ESTADO FINAL | {airflow_run_id} | lote={batch_run_id} | "
        f"{status.upper()} | lote={batch['status']} | "
        f"etapas sin terminar={pending} | s3://{BUCKET}/{key}"
    )

    if status == "partial":
        raise RunNotComplete(
            f"Lote {batch_run_id} parcial: fallaron "
            f"{batch['failed_sources']}. Los datos de las demás fuentes ya "
            "están publicados. Para recuperarlo: Clear sobre "
            "extract_rss_to_bronze con sus tareas posteriores."
        )

    if status == "failed":
        raise RunNotComplete(
            f"Procesamiento {airflow_run_id} fallido. Lote: "
            f"{batch['status']}. Etapas sin terminar: {pending}."
        )

    return record
