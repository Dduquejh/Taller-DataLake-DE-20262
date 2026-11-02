import json

import pytest

from src.pipeline import checks, ingestion
from tests.fakes import FakeS3

DATE = "2026-11-02"
STAGES_OK = {
    "extract_rss_to_bronze": "lote-1",
    "prepare_silver_news": {"received": 30},
    "publish_silver_news": {"new": 2},
    "build_gold_tables": {"partitions_rebuilt": {}},
    "validate_with_athena": {"checks": {}},
}


@pytest.fixture
def s3(monkeypatch):
    fake = FakeS3()
    for module in (ingestion, checks):
        monkeypatch.setattr(module, "s3", fake)
    return fake


def put_manifest(s3, status, failed=()):
    sources = [
        {"source_id": "medellin", "status": "success"},
        *({"source_id": name, "status": "error"} for name in failed),
    ]
    manifest = {
        "run_id": "lote-1",
        "status": status,
        "attempt": 1,
        "success_count": 1,
        "error_count": len(failed),
        "sources": sources,
    }
    s3.put_object(
        Bucket=ingestion.BUCKET,
        Key=(
            f"bronze/manifests/ingestion_date={DATE}/"
            "run_id=lote-1/manifest.json"
        ),
        Body=json.dumps(manifest).encode("utf-8"),
    )


def read_status(s3, airflow_run_id="airflow-1"):
    key = f"gold/_runs/run_id={airflow_run_id}/status.json"
    return json.loads(s3.objects[key][0])


def record(stages, airflow_run_id="airflow-1", ingestion_date=DATE):
    return checks.record_run_status(
        airflow_run_id=airflow_run_id,
        batch_run_id="lote-1",
        ingestion_date=ingestion_date,
        stages=stages,
    )


def test_complete_run_is_recorded_and_succeeds(s3):
    put_manifest(s3, "complete")

    result = record(STAGES_OK)

    assert result["status"] == "complete"
    stored = read_status(s3)
    assert stored["status"] == "complete"
    assert set(stored["stages"].values()) == {"success"}
    assert stored["batch"]["failed_sources"] == []


def test_partial_batch_is_recorded_and_fails(s3):
    put_manifest(s3, "partial", failed=["deportes"])

    with pytest.raises(checks.RunNotComplete, match="deportes"):
        record(STAGES_OK)

    stored = read_status(s3)
    assert stored["status"] == "partial"
    assert stored["batch"]["failed_sources"] == ["deportes"]


def test_unfinished_stage_marks_run_failed(s3):
    put_manifest(s3, "complete")
    stages = {**STAGES_OK, "build_gold_tables": None, "validate_with_athena": None}

    with pytest.raises(checks.RunNotComplete, match="build_gold_tables"):
        record(stages)

    stored = read_status(s3)
    assert stored["status"] == "failed"
    assert stored["stages"]["build_gold_tables"] == "not_completed"
    assert "build_gold_tables" not in stored["results"]


def test_missing_manifest_is_recorded_as_failed(s3):
    with pytest.raises(checks.RunNotComplete):
        record({"extract_rss_to_bronze": None})

    stored = read_status(s3)
    assert stored["status"] == "failed"
    assert stored["batch"]["status"] == "unknown"


def test_manifest_is_found_without_ingestion_date(s3):
    put_manifest(s3, "complete")

    result = record(STAGES_OK, ingestion_date=None)

    assert result["batch"]["status"] == "complete"


@pytest.mark.parametrize(
    ("batch", "stages", "expected"),
    [
        ("complete", STAGES_OK, "complete"),
        ("partial", STAGES_OK, "partial"),
        ("failed", STAGES_OK, "failed"),
        ("unknown", STAGES_OK, "failed"),
        ("partial", {**STAGES_OK, "prepare_silver_news": None}, "failed"),
    ],
)
def test_final_status(batch, stages, expected):
    assert checks.final_status({"status": batch}, stages) == expected


def test_validations_pass_when_every_rule_has_zero_failures(monkeypatch):
    rows = [
        {"check_name": "news_id duplicado", "failures": "0"},
        {"check_name": "entidades huérfanas", "failures": "0"},
    ]
    monkeypatch.setattr(checks.athena_queries, "query", lambda sql: (rows, 123))

    result = checks.validate_with_athena("lote-1")

    assert result == {
        "checks": {"news_id duplicado": 0, "entidades huérfanas": 0},
        "bytes_scanned": 123,
    }


def test_validations_fail_when_a_rule_has_failures(monkeypatch):
    rows = [
        {"check_name": "news_id duplicado", "failures": "2"},
        {"check_name": "entidades huérfanas", "failures": "0"},
    ]
    monkeypatch.setattr(checks.athena_queries, "query", lambda sql: (rows, 1))

    with pytest.raises(checks.ValidationFailed, match="news_id duplicado"):
        checks.validate_with_athena("lote-1")


def test_validations_file_exists():
    assert checks.VALIDATIONS_FILE.is_file()


def test_split_statements_ignores_comment_only_chunks():
    sql = "-- encabezado\nSELECT 1;\n\n-- otra\nSELECT 2\nFROM t;\n-- fin\n"

    statements = checks.athena_queries.split_statements(sql)

    assert len(statements) == 2
    assert statements[1].endswith("FROM t")
