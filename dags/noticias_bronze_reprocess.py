
from datetime import datetime, timezone

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.providers.standard.operators.python import PythonOperator

from src.pipeline.ingestion import reprocess
from src.pipeline.silver import build_silver


def run_reprocess(**context):
    """Verifica un lote existente indicado en la configuración manual."""
    dag_run = context["dag_run"]
    conf = dag_run.conf or {}
    run_id = conf.get("run_id")

    if not isinstance(run_id, str) or not run_id.strip():
        raise AirflowException(
            "Debes proporcionar run_id en la configuración del DAG."
        )

    try:
        return reprocess(run_id=run_id)
    except Exception as exc:
        raise AirflowException(
            f"No se pudo reprocesar el lote {run_id}: {exc}"
        ) from exc


def run_silver(**context):
    """Reconstruye Silver desde Bronze sin volver a consultar los RSS."""
    run_id = context["dag_run"].conf["run_id"]
    return build_silver(run_id=run_id)


with DAG(
    dag_id="noticias_bronze_reprocess",
    description="Verifica un lote de Bronze y reconstruye Silver",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["noticias", "bronze", "reprocess"],
) as dag:
    verify_existing_batch = PythonOperator(
        task_id="verify_existing_batch",
        python_callable=run_reprocess,
    )

    rebuild_silver_news = PythonOperator(
        task_id="rebuild_silver_news",
        python_callable=run_silver,
        pool="silver_writer",
    )

    verify_existing_batch >> rebuild_silver_news

