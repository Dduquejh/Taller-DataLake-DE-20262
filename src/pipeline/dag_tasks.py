"""
Funciones de las tareas de control, compartidas por los dos DAGs.

Convierten los errores definitivos en AirflowFailException: reintentar una
validación que falló o un lote que quedó parcial no cambia el resultado.
"""

import functools

from airflow.exceptions import AirflowException, AirflowFailException
from airflow.sdk import Variable

from src.pipeline import checks

# Fallo controlado para el escenario de evidencia "fallo y reejecución".
# Mientras la Variable tenga el task_id de una tarea, esa tarea falla al
# empezar, antes de leer o escribir nada:
#   airflow variables set noticias_fail_task publish_silver_news
#   airflow variables delete noticias_fail_task
# El error es reintentable: si se borra la Variable antes del siguiente
# reintento (retry_delay = 5 min), Airflow se recupera solo; si se agotan
# los reintentos, se recupera con Clear sobre la tarea y sus posteriores.
FAIL_TASK_VARIABLE = "noticias_fail_task"


class ControlledFailure(AirflowException):
    """Fallo provocado a propósito con la Variable noticias_fail_task."""


def failpoint(task_id, function):
    """Envuelve el callable de una tarea con el fallo controlado."""

    # functools.wraps conserva la firma, y Airflow pasa el contexto según
    # los parámetros de la función original.
    @functools.wraps(function)
    def wrapper(*args, **kwargs):
        if Variable.get(FAIL_TASK_VARIABLE, default=None) == task_id:
            raise ControlledFailure(
                f"Fallo controlado en {task_id} "
                f"(Variable {FAIL_TASK_VARIABLE}). Bórrala para recuperar."
            )
        return function(*args, **kwargs)

    return wrapper

# Etapas cuyo resultado se registra en el estado final, en orden.
STAGES = [
    "extract_rss_to_bronze",
    "verify_existing_batch",
    "prepare_silver_news",
    "enrich_with_comprehend",
    "publish_silver_news",
    "build_gold_tables",
    "validate_with_athena",
]


def run_preflight():
    return checks.preflight()


def run_validations(batch_run_id):
    try:
        return checks.validate_with_athena(batch_run_id)
    except checks.ValidationFailed as exc:
        raise AirflowFailException(str(exc)) from exc


def run_record_status(batch_run_id, ingestion_date=None, **context):
    """
    Lee el resultado (XCom) de cada etapa del DAG: None significa que la
    etapa no terminó, porque falló o porque no llegó a ejecutarse.
    """
    ti = context["ti"]
    task_ids = context["dag"].task_ids
    stages = {
        task_id: ti.xcom_pull(task_ids=task_id)
        for task_id in STAGES
        if task_id in task_ids
    }

    try:
        return checks.record_run_status(
            airflow_run_id=context["run_id"],
            batch_run_id=batch_run_id,
            ingestion_date=ingestion_date,
            stages=stages,
        )
    except checks.RunNotComplete as exc:
        raise AirflowFailException(str(exc)) from exc
