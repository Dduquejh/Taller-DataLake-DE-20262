
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from src.pipeline.ingestion import extract


def run_ingestion(run_id, ingestion_date):
    """Ejecuta la ingesta usando un identificador estable de Airflow."""
    return extract(
        run_id=run_id,
        ingestion_date=ingestion_date,
    )


with DAG(
    dag_id="noticias_bronze_ingestion",
    description="Captura RSS de El Tiempo y almacena originales en Bronze/S3",
    schedule="0 */3 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["noticias", "bronze", "s3"],
) as dag:
    extract_bronze = PythonOperator(
        task_id="extract_rss_to_bronze",
        python_callable=run_ingestion,
        op_kwargs={
            "run_id": "{{ run_id }}",
            "ingestion_date": "{{ dag_run.run_after | ds }}",
        },
    )

