
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from src.pipeline.dag_tasks import (
    failpoint,
    run_preflight,
    run_record_status,
    run_validations,
)
from src.pipeline.gold import build_gold
from src.pipeline.ingestion import extract
from src.pipeline.silver import enrich_silver, prepare_silver, publish_silver


def run_ingestion(run_id, ingestion_date):
    """Ejecuta la ingesta usando un identificador estable de Airflow."""
    return extract(
        run_id=run_id,
        ingestion_date=ingestion_date,
    )


with DAG(
    dag_id="noticias_bronze_ingestion",
    description="Captura RSS de El Tiempo y actualiza Bronze, Silver y Gold",
    schedule="0 */3 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["noticias", "bronze", "silver", "gold", "comprehend"],
) as dag:
    # Sin reintentos: un error de configuración o de permisos no se corrige
    # solo, y es mejor saberlo antes de consultar los RSS.
    preflight = PythonOperator(
        task_id="preflight_checks",
        python_callable=run_preflight,
        retries=0,
    )

    extract_bronze = PythonOperator(
        task_id="extract_rss_to_bronze",
        python_callable=failpoint("extract_rss_to_bronze", run_ingestion),
        op_kwargs={
            "run_id": "{{ run_id }}",
            "ingestion_date": "{{ dag_run.run_after | ds }}",
        },
    )

    prepare_silver_news = PythonOperator(
        task_id="prepare_silver_news",
        python_callable=failpoint("prepare_silver_news", prepare_silver),
        op_kwargs={
            "run_id": "{{ run_id }}",
            "ingestion_date": "{{ dag_run.run_after | ds }}",
        },
    )

    enrich_with_comprehend = PythonOperator(
        task_id="enrich_with_comprehend",
        python_callable=failpoint("enrich_with_comprehend", enrich_silver),
        op_kwargs={"run_id": "{{ run_id }}"},
    )

    # El pool de un solo cupo evita que dos DAGs reescriban Silver o Gold
    # a la vez.
    publish_silver_news = PythonOperator(
        task_id="publish_silver_news",
        python_callable=failpoint("publish_silver_news", publish_silver),
        pool="silver_writer",
        op_kwargs={"run_id": "{{ run_id }}"},
    )

    build_gold_tables = PythonOperator(
        task_id="build_gold_tables",
        python_callable=failpoint("build_gold_tables", build_gold),
        pool="silver_writer",
        op_kwargs={"run_id": "{{ run_id }}"},
    )

    validate_athena = PythonOperator(
        task_id="validate_with_athena",
        python_callable=failpoint("validate_with_athena", run_validations),
        op_kwargs={"batch_run_id": "{{ run_id }}"},
    )

    # all_done: corre aunque una tarea anterior falle, para dejar registro
    # del estado. Falla si el lote no quedó completo, así que un lote
    # parcial deja la corrida en rojo.
    record_status = PythonOperator(
        task_id="record_run_status",
        python_callable=run_record_status,
        trigger_rule=TriggerRule.ALL_DONE,
        retries=0,
        op_kwargs={
            "batch_run_id": "{{ run_id }}",
            "ingestion_date": "{{ dag_run.run_after | ds }}",
        },
    )

    (
        preflight
        >> extract_bronze
        >> prepare_silver_news
        >> enrich_with_comprehend
        >> publish_silver_news
        >> build_gold_tables
        >> validate_athena
        >> record_status
    )
