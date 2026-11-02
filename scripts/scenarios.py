"""
Crea lotes controlados en Bronze para los escenarios de evidencia.

Cada escenario escribe un lote nuevo (XML + manifiesto) a partir de las
últimas capturas reales de Bronze, con un cambio controlado, y se procesa
con el DAG de reproceso: el mismo código del pipeline, sin consultar los RSS.

Uso, dentro del contenedor de Airflow (en EC2 o en local):
    docker compose exec airflow-apiserver python scripts/scenarios.py defectuosos --trigger
    docker compose exec airflow-apiserver python scripts/scenarios.py modificada --trigger
    docker compose exec airflow-apiserver python scripts/scenarios.py parcial --trigger

También funciona desde el host con el entorno local (.venv/bin/python ...).

Escenarios:
    defectuosos  Un feed real más items inválidos (sin título, sin enlace,
                 enlace no http, sin fecha, fecha ilegible) y un item
                 repetido. Esperado: los inválidos van a cuarentena con su
                 motivo y el repetido se cuenta como duplicado del lote.
    modificada   Un feed real con el título de su primera noticia cambiado.
                 Esperado: esa noticia pasa a la versión siguiente y
                 Comprehend analiza solo ese texto.
    parcial      Dos fuentes reales y una tercera registrada con error
                 HTTP 404. Esperado: Silver y Gold se actualizan con las dos
                 fuentes y record_run_status queda en rojo (PARTIAL).

Con --trigger lanza el DAG de reproceso con docker compose; sin él, imprime
el comando.

El fallo controlado no necesita un lote; se activa con una Variable de
Airflow (ver src/pipeline/dag_tasks.py):
    docker compose exec airflow-apiserver \\
        airflow variables set noticias_fail_task publish_silver_news
"""

import argparse
import copy
import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path

from lxml import etree

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.pipeline.ingestion import (  # noqa: E402
    BUCKET,
    REGION,
    get_object,
    load_sources,
    s3,
    utc_now,
    validate_xml,
)

MANIFESTS_PREFIX = "bronze/manifests/"


def latest_captures():
    """
    Devuelve {source_id: registro del manifiesto} con la última captura
    exitosa de cada fuente, tomada de los lotes reales (no de escenarios).
    """
    manifests = []
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=BUCKET, Prefix=MANIFESTS_PREFIX):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith("/manifest.json"):
                manifests.append((obj["LastModified"], obj["Key"]))

    captures = {}

    for _, key in sorted(manifests, reverse=True):
        manifest = json.loads(get_object(key))

        if manifest.get("operation") == "scenario":
            continue

        for source in manifest.get("sources", []):
            if source.get("status") == "success":
                captures.setdefault(source["source_id"], source)

        if len(captures) == len(load_sources()):
            break

    return captures


def _item(guid, title, link, description, pub_date):
    """Construye un <item> RSS; un campo None se omite."""
    item = etree.Element("item")

    for name, value in (
        ("guid", guid),
        ("title", title),
        ("link", link),
        ("description", description),
        ("pubDate", pub_date),
    ):
        if value is not None:
            etree.SubElement(item, name).text = value

    return item


def _items(root):
    return root.xpath("//*[local-name()='item']")


def _channel(root):
    return root.xpath("//*[local-name()='channel']")[0]


def _text(item, name):
    for child in item:
        if isinstance(child.tag, str) and etree.QName(child).localname == name:
            return "".join(child.itertext())
    return None


def make_defective(content):
    """Agrega items inválidos y un item repetido a un feed real."""
    root, _ = validate_xml(content)
    channel = _channel(root)
    first = _items(root)[0]
    pub_date = _text(first, "pubDate")
    base = "https://www.eltiempo.com/escenario-defectuosos"

    channel.append(_item("esc-sin-titulo", None, f"{base}/1", "Sin título", pub_date))
    channel.append(_item("esc-sin-enlace", "Noticia sin enlace", None, "x", pub_date))
    channel.append(_item("esc-enlace-ftp", "Enlace no http", "ftp://eltiempo.com/3", "x", pub_date))
    channel.append(_item("esc-sin-fecha", "Noticia sin fecha", f"{base}/4", "x", None))
    channel.append(_item("esc-fecha-mala", "Fecha ilegible", f"{base}/5", "x", "ayer por la tarde"))
    channel.append(copy.deepcopy(first))  # repetido dentro del lote

    return etree.tostring(root, xml_declaration=True, encoding="UTF-8"), {
        "invalid_items_added": 5,
        "expected_reasons": [
            "titulo_vacio",
            "enlace_vacio",
            "enlace_invalido",
            "fecha_vacia",
            "fecha_invalida",
        ],
        "duplicated_guid": _text(first, "guid"),
    }


def make_modified(content):
    """Cambia el título de la primera noticia de un feed real."""
    root, _ = validate_xml(content)
    first = _items(root)[0]
    title = next(
        child for child in first
        if isinstance(child.tag, str) and etree.QName(child).localname == "title"
    )
    original = "".join(title.itertext())
    title.text = f"{original} (actualizado)"

    for child in list(title):
        title.remove(child)

    return etree.tostring(root, xml_declaration=True, encoding="UTF-8"), {
        "guid": _text(first, "guid"),
        "link": _text(first, "link"),
        "original_title": original,
        "new_title": title.text,
    }


def put_capture(run_id, ingestion_date, source, content, fetched_at):
    """Escribe un XML en Bronze y devuelve su registro del manifiesto."""
    key = (
        f"bronze/source={source['source_id']}/"
        f"ingestion_date={ingestion_date}/run_id={run_id}/response.xml"
    )
    digest = hashlib.sha256(content).hexdigest()
    _, news_count = validate_xml(content)

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=content,
        ContentType="application/xml",
        Metadata={"source-id": source["source_id"], "run-id": run_id, "sha256": digest},
        IfNoneMatch="*",
    )

    return {
        "source_id": source["source_id"],
        "medium": source["medium"],
        "section": source["section"],
        "url": source["url"],
        "fetched_at": fetched_at,
        "status": "success",
        "http_status": 200,
        "sha256": digest,
        "s3_key": key,
        "bytes": len(content),
        "news_count": news_count,
    }


def build_scenario(name):
    now = utc_now()
    run_id = f"escenario-{name}-{now.strftime('%Y%m%dT%H%M%SZ')}"
    ingestion_date = now.date().isoformat()
    fetched_at = now.isoformat()

    sources = {source["source_id"]: source for source in load_sources()}
    captures = latest_captures()
    ordered = [sid for sid in sources if sid in captures]

    if not ordered:
        raise RuntimeError("No hay capturas reales en Bronze para usar de base.")

    entries = []
    details = {}

    if name in ("defectuosos", "modificada"):
        source_id = ordered[0]
        content = get_object(captures[source_id]["s3_key"])
        transform = make_defective if name == "defectuosos" else make_modified
        content, details = transform(content)
        entries.append(put_capture(run_id, ingestion_date, sources[source_id], content, fetched_at))
        details["base_capture"] = captures[source_id]["s3_key"]

    elif name == "parcial":
        if len(ordered) < 2:
            raise RuntimeError("Se necesitan al menos 2 fuentes con capturas.")

        failed_id = ordered[-1]

        for source_id in ordered[:-1]:
            content = get_object(captures[source_id]["s3_key"])
            entries.append(put_capture(run_id, ingestion_date, sources[source_id], content, fetched_at))

        failed = sources[failed_id]
        entries.append({
            "source_id": failed_id,
            "medium": failed["medium"],
            "section": failed["section"],
            "url": failed["url"],
            "fetched_at": fetched_at,
            "status": "error",
            "http_status": 404,
            "error": "HTTPError: 404 Client Error (simulado por el escenario)",
        })
        details["failed_source"] = failed_id

    success = sum(entry["status"] == "success" for entry in entries)
    manifest = {
        "run_id": run_id,
        "ingestion_date": ingestion_date,
        "started_at": fetched_at,
        "finished_at": utc_now().isoformat(),
        "bucket": BUCKET,
        "region": REGION,
        "operation": "scenario",
        "scenario": name,
        "scenario_details": details,
        "attempt": 1,
        "previous_status": None,
        "sources": entries,
        "success_count": success,
        "error_count": len(entries) - success,
        "status": "complete" if success == len(entries) else "partial",
    }

    manifest_key = (
        f"{MANIFESTS_PREFIX}ingestion_date={ingestion_date}/"
        f"run_id={run_id}/manifest.json"
    )
    s3.put_object(
        Bucket=BUCKET,
        Key=manifest_key,
        Body=json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json",
        IfNoneMatch="*",
    )

    print(f"ESCENARIO | {name} | run_id={run_id} | estado={manifest['status']}")
    print(f"Manifiesto: s3://{BUCKET}/{manifest_key}")
    print(json.dumps(details, ensure_ascii=False, indent=2))

    return run_id


def trigger_command(run_id):
    """Dentro del contenedor usa la CLI de Airflow; desde el host, compose."""
    command = [
        "airflow", "dags", "trigger", "noticias_bronze_reprocess",
        "--conf", json.dumps({"run_id": run_id}),
    ]

    if shutil.which("airflow"):
        return command

    return ["docker", "compose", "exec", "-T", "airflow-apiserver", *command]


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("scenario", choices=["defectuosos", "modificada", "parcial"])
    parser.add_argument("--trigger", action="store_true", help="lanza el DAG de reproceso")
    args = parser.parse_args()

    run_id = build_scenario(args.scenario)
    command = trigger_command(run_id)

    if args.trigger:
        subprocess.run(command, check=True)
    else:
        print("\nPara procesarlo:")
        print(" ".join(command[:-1]) + f" '{command[-1]}'")


if __name__ == "__main__":
    main()
