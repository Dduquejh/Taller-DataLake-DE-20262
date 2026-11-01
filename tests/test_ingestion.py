import json

import pytest
import requests
from botocore.exceptions import ClientError

from src.pipeline import ingestion
from tests.fakes import FakeS3


FEED = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel><title>Feed</title>
  <item><title>Noticia</title><link>https://ejemplo.com/a</link></item>
</channel></rss>"""

SOURCES = [
    {
        "source_id": "fuente_a",
        "medium": "El Tiempo",
        "section": "Medellín",
        "url": "https://ejemplo.com/a.xml",
    },
    {
        "source_id": "fuente_b",
        "medium": "El Tiempo",
        "section": "Deportes",
        "url": "https://ejemplo.com/b.xml",
    },
]

RUN_ID = "run-1"
DATE = "2026-11-01"
MANIFEST_KEY = (
    f"bronze/manifests/ingestion_date={DATE}/run_id={RUN_ID}/manifest.json"
)


class ManifestS3(FakeS3):
    def manifest(self):
        return json.loads(self.objects[MANIFEST_KEY][0])


class FakeResponse:
    def __init__(self, status_code=200, content=FEED):
        self.status_code = status_code
        self.content = content
        self.headers = {"Content-Type": "application/rss+xml"}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")


@pytest.fixture
def s3(monkeypatch):
    fake = ManifestS3()
    monkeypatch.setattr(ingestion, "s3", fake)
    monkeypatch.setattr(ingestion, "load_sources", lambda: SOURCES)
    monkeypatch.setattr(ingestion.time, "sleep", lambda seconds: None)
    return fake


@pytest.fixture
def responses(monkeypatch):
    """Respuestas por URL; cada lista se consume en orden."""
    plan = {}
    calls = []

    def fake_get(url, **kwargs):
        calls.append(url)
        outcome = plan[url].pop(0) if len(plan[url]) > 1 else plan[url][0]

        if isinstance(outcome, Exception):
            raise outcome

        return outcome

    monkeypatch.setattr(ingestion.requests, "get", fake_get)
    return plan, calls


def test_all_sources_ok_is_complete(s3, responses):
    plan, _ = responses
    plan["https://ejemplo.com/a.xml"] = [FakeResponse()]
    plan["https://ejemplo.com/b.xml"] = [FakeResponse()]

    ingestion.extract(RUN_ID, DATE)

    manifest = s3.manifest()
    assert manifest["status"] == "complete"
    assert manifest["success_count"] == 2
    assert manifest["error_count"] == 0
    assert manifest["attempt"] == 1


def test_one_source_down_is_partial_and_does_not_raise(s3, responses):
    plan, _ = responses
    plan["https://ejemplo.com/a.xml"] = [FakeResponse()]
    plan["https://ejemplo.com/b.xml"] = [FakeResponse(status_code=404)]

    assert ingestion.extract(RUN_ID, DATE) == RUN_ID

    manifest = s3.manifest()
    assert manifest["status"] == "partial"
    failed = manifest["sources"][1]
    assert failed["status"] == "error"
    assert failed["http_status"] == 404
    assert "HTTPError" in failed["error"]


def test_all_sources_down_is_failed_and_raises(s3, responses):
    plan, _ = responses
    plan["https://ejemplo.com/a.xml"] = [FakeResponse(status_code=500)]
    plan["https://ejemplo.com/b.xml"] = [requests.ConnectionError("caído")]

    with pytest.raises(RuntimeError, match="fallido"):
        ingestion.extract(RUN_ID, DATE)

    assert s3.manifest()["status"] == "failed"


def test_empty_response_is_an_explicit_error(s3, responses):
    plan, _ = responses
    plan["https://ejemplo.com/a.xml"] = [FakeResponse()]
    plan["https://ejemplo.com/b.xml"] = [FakeResponse(content=b"  \n")]

    ingestion.extract(RUN_ID, DATE)

    failed = s3.manifest()["sources"][1]
    assert failed["status"] == "error"
    assert "vacía" in failed["error"]


def test_transient_errors_are_retried_within_the_source(s3, responses):
    plan, calls = responses
    plan["https://ejemplo.com/a.xml"] = [
        requests.Timeout("lento"),
        FakeResponse(status_code=503),
        FakeResponse(),
    ]
    plan["https://ejemplo.com/b.xml"] = [FakeResponse()]

    ingestion.extract(RUN_ID, DATE)

    assert s3.manifest()["status"] == "complete"
    assert calls.count("https://ejemplo.com/a.xml") == 3


def test_client_errors_are_not_retried(s3, responses):
    plan, calls = responses
    plan["https://ejemplo.com/a.xml"] = [FakeResponse()]
    plan["https://ejemplo.com/b.xml"] = [FakeResponse(status_code=404)]

    ingestion.extract(RUN_ID, DATE)

    assert calls.count("https://ejemplo.com/b.xml") == 1


def test_rerun_of_partial_batch_only_fetches_failed_source(s3, responses):
    plan, calls = responses
    plan["https://ejemplo.com/a.xml"] = [FakeResponse()]
    plan["https://ejemplo.com/b.xml"] = [FakeResponse(status_code=404)]
    ingestion.extract(RUN_ID, DATE)
    first_a_key = s3.manifest()["sources"][0]["s3_key"]

    calls.clear()
    plan["https://ejemplo.com/b.xml"] = [FakeResponse()]
    ingestion.extract(RUN_ID, DATE)

    manifest = s3.manifest()
    assert manifest["status"] == "complete"
    assert manifest["attempt"] == 2
    assert manifest["previous_status"] == "partial"
    assert calls == ["https://ejemplo.com/b.xml"]
    assert manifest["sources"][0]["s3_key"] == first_a_key
    assert manifest["sources"][0]["reused_existing_object"] is True


def test_rerun_of_failed_batch_recovers(s3, responses):
    plan, _ = responses
    plan["https://ejemplo.com/a.xml"] = [FakeResponse(status_code=500)]
    plan["https://ejemplo.com/b.xml"] = [FakeResponse(status_code=500)]

    with pytest.raises(RuntimeError):
        ingestion.extract(RUN_ID, DATE)

    plan["https://ejemplo.com/a.xml"] = [FakeResponse()]
    plan["https://ejemplo.com/b.xml"] = [FakeResponse()]
    ingestion.extract(RUN_ID, DATE)

    assert s3.manifest()["status"] == "complete"


def test_complete_batch_is_not_fetched_again(s3, responses):
    plan, calls = responses
    plan["https://ejemplo.com/a.xml"] = [FakeResponse()]
    plan["https://ejemplo.com/b.xml"] = [FakeResponse()]
    ingestion.extract(RUN_ID, DATE)
    versions = s3.versions

    calls.clear()
    ingestion.extract(RUN_ID, DATE)

    assert calls == []
    assert s3.versions == versions


def test_concurrent_manifest_change_is_not_overwritten(s3, responses):
    plan, _ = responses
    plan["https://ejemplo.com/a.xml"] = [FakeResponse()]
    plan["https://ejemplo.com/b.xml"] = [FakeResponse(status_code=404)]
    ingestion.extract(RUN_ID, DATE)

    # Otro proceso reescribe el manifiesto mientras este reintenta.
    original_get = s3.get_object

    def get_then_race(Bucket, Key):
        response = original_get(Bucket, Key)
        if Key == MANIFEST_KEY:
            body, _ = s3.objects[Key]
            s3.versions += 1
            s3.objects[Key] = (body, f'"v{s3.versions}"')
        return response

    s3.get_object = get_then_race
    plan["https://ejemplo.com/b.xml"] = [FakeResponse()]

    with pytest.raises(ClientError):
        ingestion.extract(RUN_ID, DATE)


@pytest.mark.parametrize(
    "counts, expected",
    [((2, 0), "complete"), ((1, 1), "partial"), ((0, 2), "failed")],
)
def test_batch_status(counts, expected):
    success, errors = counts
    manifest = {
        "sources": [{"status": "success"}] * success
        + [{"status": "error"}] * errors
    }
    assert ingestion.batch_status(manifest) == expected
