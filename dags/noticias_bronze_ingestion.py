
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

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
    description="Captura RSS de El Tiempo en Bronze, enriquece y publica Silver",
    schedule="0 */3 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["noticias", "bronze", "silver", "comprehend"],
) as dag:
    extract_bronze = PythonOperator(
        task_id="extract_rss_to_bronze",
        python_callable=run_ingestion,
        op_kwargs={
            "run_id": "{{ run_id }}",
            "ingestion_date": "{{ dag_run.run_after | ds }}",
        },
    )

    prepare_silver_news = PythonOperator(
        task_id="prepare_silver_news",
        python_callable=prepare_silver,
        op_kwargs={
            "run_id": "{{ run_id }}",
            "ingestion_date": "{{ dag_run.run_after | ds }}",
        },
    )

    enrich_with_comprehend = PythonOperator(
        task_id="enrich_with_comprehend",
        python_callable=enrich_silver,
        op_kwargs={"run_id": "{{ run_id }}"},
    )

    # El pool de un solo cupo evita que dos DAGs reescriban Silver a la vez.
    publish_silver_news = PythonOperator(
        task_id="publish_silver_news",
        python_callable=publish_silver,
        pool="silver_writer",
        op_kwargs={"run_id": "{{ run_id }}"},
    )

    (
        extract_bronze
        >> prepare_silver_news
        >> enrich_with_comprehend
        >> publish_silver_news
    )
