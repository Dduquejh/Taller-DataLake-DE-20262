"""
Ejecuta en Athena las consultas de uno o varios archivos .sql.

Uso:
    python scripts/run_athena.py sql/02_top_organizaciones.sql
    python scripts/run_athena.py sql/*.sql

Cada archivo puede tener varias consultas separadas por ";" al final de una
línea. Por cada consulta imprime las filas (hasta --max-rows), el estado y
los bytes escaneados.

Variables de entorno (con los valores que crea terraform/catalog):
    AWS_REGION        us-east-2
    ATHENA_WORKGROUP  noticias-data-lake
    ATHENA_DATABASE   noticias_data_lake
"""

import argparse
import os
import re
import sys
import time

import boto3

REGION = os.getenv("AWS_REGION", "us-east-2")
WORKGROUP = os.getenv("ATHENA_WORKGROUP", "noticias-data-lake")
DATABASE = os.getenv("ATHENA_DATABASE", "noticias_data_lake")

athena = boto3.client("athena", region_name=REGION)


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


def run_query(sql):
    """Ejecuta una consulta y espera a que termine. Devuelve su ejecución."""
    query_id = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE},
        WorkGroup=WORKGROUP,
    )["QueryExecutionId"]

    while True:
        execution = athena.get_query_execution(
            QueryExecutionId=query_id
        )["QueryExecution"]

        if execution["Status"]["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            return execution

        time.sleep(1)


def fetch_rows(query_id, max_rows):
    """Devuelve el encabezado y hasta max_rows filas del resultado."""
    paginator = athena.get_paginator("get_query_results")
    rows = []

    for page in paginator.paginate(QueryExecutionId=query_id):
        for row in page["ResultSet"]["Rows"]:
            rows.append([cell.get("VarCharValue", "") for cell in row["Data"]])

            if len(rows) > max_rows:
                return rows[0], rows[1:]

    return (rows[0], rows[1:]) if rows else ([], [])


def print_table(header, rows):
    widths = [
        max(len(str(value)) for value in column)
        for column in zip(header, *rows)
    ]

    for line in [header, ["-" * w for w in widths], *rows]:
        print("  ".join(str(v).ljust(w) for v, w in zip(line, widths)))


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("files", nargs="+")
    parser.add_argument("--max-rows", type=int, default=50)
    args = parser.parse_args()

    failed = False

    for path in args.files:
        with open(path, encoding="utf-8") as f:
            statements = split_statements(f.read())

        for number, sql in enumerate(statements, start=1):
            execution = run_query(sql)
            status = execution["Status"]
            scanned = execution["Statistics"].get("DataScannedInBytes", 0)

            print(f"\n=== {path} [{number}/{len(statements)}] "
                  f"{status['State']} | {scanned} bytes escaneados")

            if status["State"] != "SUCCEEDED":
                print(status.get("StateChangeReason", ""))
                failed = True
                continue

            header, rows = fetch_rows(execution["QueryExecutionId"], args.max_rows)
            print_table(header, rows) if rows else print("(sin filas)")

    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
