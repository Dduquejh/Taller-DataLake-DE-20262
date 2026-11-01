import hashlib
import io
import json
from datetime import datetime, timedelta, timezone
from xml.sax.saxutils import escape

import pyarrow.parquet as pq
import pytest

from src.pipeline import enrichment, ingestion, silver
from tests.fakes import FakeComprehend, FakeS3


DATE = "2026-11-01"
T0 = datetime(2026, 11, 1, 6, 0, tzinfo=timezone.utc)


@pytest.fixture
def s3(monkeypatch):
    fake = FakeS3()
    monkeypatch.setattr(ingestion, "s3", fake)
    monkeypatch.setattr(silver, "s3", fake)
    return fake


@pytest.fixture
def comprehend(monkeypatch):
    fake = FakeComprehend()
    monkeypatch.setattr(enrichment, "comprehend", fake)
    return fake


def feed(*items):
    """RSS con items (guid, título, descripción)."""
    body = "".join(
        f"<item><guid>{guid}</guid><title>{escape(title)}</title>"
        f"<link>https://ejemplo.com/{guid}</link>"
        f"<description>{escape(description)}</description>"
        "<pubDate>Sun, 01 Nov 2026 01:00:00 -0500</pubDate></item>"
        for guid, title, description in items
    )
    return (
        '<?xml version="1.0" encoding="UTF-8"?><rss version="2.0">'
        f"<channel><title>Feed</title>{body}</channel></rss>"
    ).encode("utf-8")


def put_batch(s3, run_id, items, hours=0):
    """Escribe en Bronze un lote completo de una sola fuente."""
    content = feed(*items)
    key = (
        f"bronze/source=fuente_a/ingestion_date={DATE}/"
        f"run_id={run_id}/response.xml"
    )
    s3.put_object(Bucket="b", Key=key, Body=content)
    manifest = {
        "run_id": run_id,
        "ingestion_date": DATE,
        "status": "complete",
        "sources": [{
            "source_id": "fuente_a",
            "medium": "El Tiempo",
            "section": "Medellín",
            "status": "success",
            "s3_key": key,
            "sha256": hashlib.sha256(content).hexdigest(),
            "fetched_at": (T0 + timedelta(hours=hours)).isoformat(),
        }],
    }
    s3.put_object(
        Bucket="b",
        Key=(
            f"bronze/manifests/ingestion_date={DATE}/"
            f"run_id={run_id}/manifest.json"
        ),
        Body=json.dumps(manifest).encode("utf-8"),
    )


def run(s3, run_id, items, hours=0):
    put_batch(s3, run_id, items, hours)
    return silver.build_silver(run_id, DATE)


def table(s3, prefix, file_name):
    rows = []

    for key, (body, _) in sorted(s3.objects.items()):
        if key.startswith(prefix) and key.endswith(file_name):
            partition = silver._partition_from_key(key)
            for row in pq.read_table(io.BytesIO(body)).to_pylist():
                rows.append({**row, "published_date": partition})

    return rows


def news(s3):
    return {
        row["news_id"]: row
        for row in table(s3, silver.NEWS_PREFIX, silver.NEWS_FILE)
    }


def entities(s3):
    return table(s3, silver.ENTITIES_PREFIX, silver.ENTITIES_FILE)


def key_phrases(s3):
    return table(s3, silver.KEY_PHRASES_PREFIX, silver.KEY_PHRASES_FILE)


def news_id(guid):
    return silver.make_news_id(guid, None)


ITEM_A = ("a", "Ecopetrol invierte en Medellín",
          "La empresa anunció obras en Medellín para 2026.")
ITEM_B = ("b", "Federico Gutiérrez habla de seguridad", "Rueda de prensa.")


# --- enrichment.distinct_mentions -------------------------------------------

def test_distinct_mentions_groups_case_and_keeps_max_score():
    result = enrichment.distinct_mentions(
        [
            {"Text": "Medellín", "Type": "LOCATION", "Score": 0.7},
            {"Text": "MEDELLÍN ", "Type": "LOCATION", "Score": 0.9},
            {"Text": "Ecopetrol", "Type": "ORGANIZATION", "Score": 0.8},
            {"Text": "2026", "Type": "DATE", "Score": 0.99},
        ],
        types=enrichment.ENTITY_TYPES,
    )

    # Ordenado por tipo y texto; DATE queda fuera.
    assert [(item["text"], item["mention_count"]) for item in result] == [
        ("Medellín", 2),
        ("Ecopetrol", 1),
    ]
    assert result[0]["score"] == 0.9
    assert result[0]["normalized"] == "medellín"


# --- enrichment.analyze_texts -----------------------------------------------

def test_analyze_texts_statuses(comprehend):
    results, stats = enrichment.analyze_texts({
        "vacio": "  ",
        "largo": "x" * (enrichment.MAX_TEXT_BYTES + 1),
        "frances": "Bonjour Medellín",
        "ingles": "Hello from Bogotá",
        "falla": "FALLA en Medellín",
        "ok": "Ecopetrol en Medellín",
    })

    assert {key: value["status"] for key, value in results.items()} == {
        "vacio": "empty_text",
        "largo": "too_long",
        "frances": "unsupported_language",
        "ingles": "analyzed",
        "falla": "error",
        "ok": "analyzed",
    }
    assert results["ingles"]["language"] == "en"
    assert results["frances"]["language"] == "fr"
    assert "falla simulada" in results["falla"]["error"]
    assert {item["text"] for item in results["ok"]["entities"]} == {
        "Ecopetrol",
        "Medellín",
    }
    # Solo se envían a entidades los textos válidos en idiomas contemplados.
    assert "  " not in comprehend.sent
    assert not any(text.startswith("Bonjour") for text in comprehend.sent)
    assert stats["api_calls"]["detect_entities"] == 2  # un lote por idioma


def test_analyze_texts_splits_batches_of_25(comprehend):
    texts = {f"h{index}": f"Noticia {index}" for index in range(30)}

    results, stats = enrichment.analyze_texts(texts)

    assert len(results) == 30
    assert stats["api_calls"] == {
        "detect_dominant_language": 2,
        "detect_entities": 2,
        "detect_key_phrases": 2,
    }


# --- Silver de punta a punta -------------------------------------------------

def test_first_batch_creates_news_entities_and_key_phrases(s3, comprehend):
    summary = run(s3, "run-1", [ITEM_A, ITEM_B])

    current = news(s3)
    assert {row["analysis_status"] for row in current.values()} == {"analyzed"}
    row = current[news_id("a")]
    assert row["analyzed_text"] == f"{ITEM_A[1]}\n{ITEM_A[2]}"
    assert row["analyzed_text_hash"] == row["content_hash"]
    assert row["analysis_language"] == "es"

    found = {
        (item["news_id"], item["entity_text"], item["entity_type"]):
            item["mention_count"]
        for item in entities(s3)
    }
    assert found == {
        (news_id("a"), "Ecopetrol", "ORGANIZATION"): 1,
        (news_id("a"), "Medellín", "LOCATION"): 2,
        (news_id("b"), "Federico Gutiérrez", "PERSON"): 1,
    }
    assert {item["key_phrase"] for item in key_phrases(s3)} == {
        ITEM_A[1],
        ITEM_B[1],
    }
    assert summary["analysis"] == {"applied_analyzed": 2}
    assert summary["current_entities_total"] == 3


def test_repeated_batch_makes_no_comprehend_calls(s3, comprehend):
    run(s3, "run-1", [ITEM_A, ITEM_B])
    calls = dict(comprehend.calls)

    summary = run(s3, "run-2", [ITEM_A, ITEM_B], hours=3)

    assert dict(comprehend.calls) == calls
    assert summary["repeated"] == 2
    assert summary["partitions_written"] == []


def test_enrich_retry_reuses_staged_results(s3, comprehend):
    put_batch(s3, "run-1", [ITEM_A, ITEM_B])
    silver.prepare_silver("run-1", DATE)
    first = silver.enrich_silver("run-1")
    second = silver.enrich_silver("run-1")

    assert first["texts_sent"] == 2
    assert second["texts_sent"] == 0
    assert second["texts_reused_from_retry"] == 2
    assert comprehend.calls["detect_entities"] == 1


def test_modified_news_is_reanalyzed_and_old_entities_removed(s3, comprehend):
    run(s3, "run-1", [ITEM_A, ITEM_B])
    sent_before = len(comprehend.sent)

    changed = ("a", "Ecopetrol invierte en Bogotá", "Cambio de sede.")
    summary = run(s3, "run-2", [changed, ITEM_B], hours=3)

    assert summary["updated"] == 1
    assert summary["repeated"] == 1
    assert comprehend.sent[sent_before:] == [f"{changed[1]}\n{changed[2]}"]

    row = news(s3)[news_id("a")]
    assert row["version"] == 2
    assert row["analyzed_text_hash"] == row["content_hash"]

    names = {
        item["entity_text"]
        for item in entities(s3)
        if item["news_id"] == news_id("a")
    }
    assert names == {"Ecopetrol", "Bogotá"}


def test_failed_document_is_retried_on_next_run(s3, comprehend):
    failing = ("c", "FALLA en Medellín", "")
    run(s3, "run-1", [failing])
    assert news(s3)[news_id("c")]["analysis_status"] == "error"

    run(s3, "run-2", [failing], hours=3)

    # El texto sigue fallando, pero se volvió a intentar.
    assert comprehend.sent.count("FALLA en Medellín\n") == 2


def test_unsupported_language_is_final_and_not_retried(s3, comprehend):
    foreign = ("d", "Bonjour Medellín", "")
    run(s3, "run-1", [foreign])
    run(s3, "run-2", [foreign], hours=3)

    row = news(s3)[news_id("d")]
    assert row["analysis_status"] == "unsupported_language"
    assert row["analysis_language"] == "fr"
    assert comprehend.calls["detect_dominant_language"] == 1
    assert entities(s3) == []


def test_publish_without_enrichment_marks_pending_then_recovers(
    s3, comprehend,
):
    put_batch(s3, "run-1", [ITEM_A])
    silver.prepare_silver("run-1", DATE)
    summary = silver.publish_silver("run-1")

    assert summary["analysis"] == {"pending": 1}
    assert news(s3)[news_id("a")]["analysis_status"] == "pending"
    assert entities(s3) == []

    run(s3, "run-2", [ITEM_A], hours=3)

    assert news(s3)[news_id("a")]["analysis_status"] == "analyzed"
    assert len(entities(s3)) == 2


def test_existing_news_without_analysis_are_backfilled(s3, comprehend):
    run(s3, "run-1", [ITEM_A])

    # Simula una noticia escrita antes de existir el enriquecimiento.
    row = news(s3)[news_id("a")]
    legacy = {
        key: value for key, value in row.items()
        if not key.startswith("analy") and key != "published_date"
    }
    silver._write_table(
        f"{silver.NEWS_PREFIX}published_date={row['published_date']}/"
        f"{silver.NEWS_FILE}",
        [legacy],
        silver.NEWS_SCHEMA,
    )
    for key in [key for key in s3.objects if "news_entities" in key]:
        del s3.objects[key]

    run(s3, "run-2", [ITEM_B], hours=3)

    current = news(s3)
    assert current[news_id("a")]["analysis_status"] == "analyzed"
    assert current[news_id("b")]["analysis_status"] == "analyzed"


def test_every_entity_and_phrase_belongs_to_current_news(s3, comprehend):
    run(s3, "run-1", [ITEM_A, ITEM_B])
    run(
        s3,
        "run-2",
        [("a", "Ecopetrol invierte en Bogotá", "Cambio."), ITEM_B],
        hours=3,
    )

    current = news(s3)

    for item in entities(s3) + key_phrases(s3):
        owner = current[item["news_id"]]
        assert item["published_date"] == owner["published_date"]
        assert item["text_hash"] == owner["analyzed_text_hash"]

    pairs = [
        (item["news_id"], item["entity_type"], item["entity_text_normalized"])
        for item in entities(s3)
    ]
    assert len(pairs) == len(set(pairs))
