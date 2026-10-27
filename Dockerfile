FROM apache/airflow:3.3.2-python3.12

WORKDIR /opt/airflow

COPY requirements.txt /opt/airflow/requirements.txt

RUN pip install --no-cache-dir \
    --constraint "${HOME}/constraints.txt" \
    boto3 \
    requests \
    PyYAML \
    python-dotenv \
    lxml

COPY src/ /opt/airflow/src/
COPY config/ /opt/airflow/config/

ENV PYTHONPATH=/opt/airflow
ENV PYTHONUNBUFFERED=1