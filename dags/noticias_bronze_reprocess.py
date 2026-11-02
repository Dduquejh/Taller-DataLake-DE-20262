
from datetime import datetime, timezone

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from src.pipeline.dag_tasks import (
    failpoint,
    run_preflight,
    run_record_status,
    run_validations,
)
from src.pipeline.gold import build_gold
from src.pipeline.ingestion import reprocess
from src.pipeline.silver import enrich_silver, prepare_silver, publish_silver


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


def run_prepare(**context):
    """Prepara el lote desde Bronze sin volver a consultar los RSS."""
    return prepare_silver(run_id=context["dag_run"].conf["run_id"])


def run_enrich(**context):
    """Analiza solo los textos que aún no tienen un análisis vigente."""
    return enrich_silver(run_id=context["dag_run"].conf["run_id"])


def run_publish(**context):
    """Publica el lote reprocesado en Silver."""
    return publish_silver(run_id=context["dag_run"].conf["run_id"])


def run_gold(**context):
    """
    Actualiza Gold. Con {"full_gold": true} en la configuración se
    reconstruyen todas las particiones, no solo las desactualizadas.
    """
    conf = context["dag_run"].conf
    return build_gold(
        run_id=context["dag_run"].run_id,
        full=bool(conf.get("full_gold", False)),
    )


with DAG(
    dag_id="noticias_bronze_reprocess",
    description="Verifica un lote de Bronze y reconstruye Silver y Gold",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["noticias", "bronze", "reprocess"],
) as dag:
    preflight = PythonOperator(
        task_id="preflight_checks",
        python_callable=run_preflight,
        retries=0,
    )

    verify_existing_batch = PythonOperator(
        task_id="verify_existing_batch",
        python_callable=failpoint("verify_existing_batch", run_reprocess),
    )

    prepare_silver_news = PythonOperator(
        task_id="prepare_silver_news",
        python_callable=failpoint("prepare_silver_news", run_prepare),
    )

    enrich_with_comprehend = PythonOperator(
        task_id="enrich_with_comprehend",
        python_callable=failpoint("enrich_with_comprehend", run_enrich),
    )

    publish_silver_news = PythonOperator(
        task_id="publish_silver_news",
        python_callable=failpoint("publish_silver_news", run_publish),
        pool="silver_writer",
    )

    build_gold_tables = PythonOperator(
        task_id="build_gold_tables",
        python_callable=failpoint("build_gold_tables", run_gold),
        pool="silver_writer",
    )

    validate_athena = PythonOperator(
        task_id="validate_with_athena",
        python_callable=failpoint("validate_with_athena", run_validations),
        op_kwargs={"batch_run_id": "{{ dag_run.conf.get('run_id', '') }}"},
    )

    # Ver noticias_bronze_ingestion: registra el estado aunque algo falle.
    # Sin ingestion_date, el manifiesto se busca por run_id.
    record_status = PythonOperator(
        task_id="record_run_status",
        python_callable=run_record_status,
        trigger_rule=TriggerRule.ALL_DONE,
        retries=0,
        op_kwargs={"batch_run_id": "{{ dag_run.conf.get('run_id', '') }}"},
    )

    (
        preflight
        >> verify_existing_batch
        >> prepare_silver_news
        >> enrich_with_comprehend
        >> publish_silver_news
        >> build_gold_tables
        >> validate_athena
        >> record_status
    )

