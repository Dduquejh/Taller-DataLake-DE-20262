"""
Ejecuta en Athena las consultas de uno o varios archivos .sql.

Uso (desde la raíz del repositorio):
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
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.pipeline.athena import fetch_rows, split_statements, start_and_wait  # noqa: E402


def print_table(header, rows):
    rows = [["" if value is None else value for value in row] for row in rows]
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
            execution = start_and_wait(sql)
            status = execution["Status"]
            scanned = execution["Statistics"].get("DataScannedInBytes", 0)

            print(f"\n=== {path} [{number}/{len(statements)}] "
                  f"{status['State']} | {scanned} bytes escaneados")

            if status["State"] != "SUCCEEDED":
                print(status.get("StateChangeReason", ""))
                failed = True
                continue

            header, rows = fetch_rows(
                execution["QueryExecutionId"], args.max_rows
            )

            if rows:
                print_table(header, rows)
            else:
                print("(sin filas)")

    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
