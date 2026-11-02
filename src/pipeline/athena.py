"""
Ejecución de consultas en Athena sobre el catálogo de terraform/catalog.

Lo usan las validaciones del DAG (src/pipeline/checks.py) y
scripts/run_athena.py.
"""

import os
import re
import time

import boto3

# No se importa de ingestion para no exigir S3_BUCKET al usar solo Athena.
REGION = os.getenv("AWS_REGION", "us-east-2")
WORKGROUP = os.getenv("ATHENA_WORKGROUP", "noticias-data-lake")
DATABASE = os.getenv("ATHENA_DATABASE", "noticias_data_lake")
QUERY_TIMEOUT_SECONDS = 300

athena = boto3.Session(region_name=REGION).client("athena")


class QueryFailed(RuntimeError):
    """Una consulta de Athena terminó en FAILED o CANCELLED."""


def split_statements(sql):
    """Separa las consultas por ';' a final de línea y quita las vacías."""
    statements = []

    for chunk in re.split(r";\s*$", sql, flags=re.M):
        code = "\n".join(
            line for line in chunk.splitlines()
            if not line.strip().startswith("--")
        ).strip()

        if code:
            statements.append(chunk.strip())

    return statements


def start_and_wait(sql, timeout=QUERY_TIMEOUT_SECONDS):
    """Ejecuta una consulta y espera a que termine. Devuelve su ejecución."""
    query_id = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE},
        WorkGroup=WORKGROUP,
    )["QueryExecutionId"]
    deadline = time.monotonic() + timeout

    while True:
        execution = athena.get_query_execution(
            QueryExecutionId=query_id
        )["QueryExecution"]
        state = execution["Status"]["State"]

        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            return execution

        if time.monotonic() > deadline:
            athena.stop_query_execution(QueryExecutionId=query_id)
            raise TimeoutError(
                f"La consulta {query_id} superó {timeout} s y se detuvo."
            )

        time.sleep(1)


def fetch_rows(query_id, max_rows=None):
    """Devuelve el encabezado y las filas (como texto) de un resultado."""
    paginator = athena.get_paginator("get_query_results")
    rows = []

    for page in paginator.paginate(QueryExecutionId=query_id):
        for row in page["ResultSet"]["Rows"]:
            rows.append([cell.get("VarCharValue") for cell in row["Data"]])

            if max_rows is not None and len(rows) > max_rows:
                return rows[0], rows[1:]

    return (rows[0], rows[1:]) if rows else ([], [])


def query(sql, max_rows=None):
    """
    Ejecuta una consulta y devuelve (filas como dicts, bytes escaneados).

    Lanza QueryFailed si Athena no la completa.
    """
    execution = start_and_wait(sql)
    status = execution["Status"]

    if status["State"] != "SUCCEEDED":
        raise QueryFailed(
            f"Athena {status['State']}: {status.get('StateChangeReason', '')}"
        )

    header, rows = fetch_rows(execution["QueryExecutionId"], max_rows)
    scanned = execution["Statistics"].get("DataScannedInBytes", 0)

    return [dict(zip(header, row)) for row in rows], scanned
