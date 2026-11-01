import hashlib
import json
from datetime import datetime, timedelta, timezone
from xml.sax.saxutils import escape

import pytest

from src.pipeline import enrichment, gold, ingestion, silver
from tests.fakes import FakeComprehend, FakeS3
from tests.test_enrichment import table


DATE = "2026-11-01"
T0 = datetime(2026, 11, 1, 6, 0, tzinfo=timezone.utc)
PUB_DATE = "Sun, 01 Nov 2026 01:00:00 -0500"  # 2026-11-01 06:00 UTC


@pytest.fixture
def s3(monkeypatch):
    fake = FakeS3()
    for module in (ingestion, silver, gold):
        monkeypatch.setattr(module, "s3", fake)
    monkeypatch.setattr(enrichment, "comprehend", FakeComprehend())
    return fake


def run_silver(s3, run_id, items, hours=0, section="Medellín"):
    """Escribe un lote en Bronze y lo publica en Silver.

    items: (guid, título, descripción, pubDate).
    """
    body = "".join(
        f"<item><guid>{guid}</guid><title>{escape(title)}</title>"
        f"<link>https://ejemplo.com/{guid}</link>"
        f"<description>{escape(description)}</description>"
        f"<pubDate>{pub_date}</pubDate></item>"
        for guid, title, description, pub_date in items
    )
    content = (
        '<?xml version="1.0" encoding="UTF-8"?><rss version="2.0">'
        f"<channel><title>Feed</title>{body}</channel></rss>"
    ).encode("utf-8")
    key = f"bronze/source=a/ingestion_date={DATE}/run_id={run_id}/response.xml"
    s3.put_object(Bucket="b", Key=key, Body=content)
    manifest = {
        "run_id": run_id,
        "ingestion_date": DATE,
        "status": "complete",
        "sources": [{
            "source_id": "a",
            "medium": "El Tiempo",
            "section": section,
            "status": "success",
            "s3_key": key,
            "sha256": hashlib.sha256(content).hexdigest(),
            "fetched_at": (T0 + timedelta(hours=hours)).isoformat(),
        }],
    }
    s3.put_object(
        Bucket="b",
        Key=f"bronze/manifests/ingestion_date={DATE}/run_id={run_id}/manifest.json",
        Body=json.dumps(manifest).encode("utf-8"),
    )
    return silver.build_silver(run_id, DATE)


def gold_rows(s3, prefix, file_name):
    return table(s3, prefix, file_name)


def news_daily(s3):
    return gold_rows(s3, gold.NEWS_DAILY_PREFIX, gold.NEWS_DAILY_FILE)


def entity_daily(s3):
    return gold_rows(s3, gold.ENTITY_DAILY_PREFIX, gold.ENTITY_DAILY_FILE)


def key_phrase_daily(s3):
    return gold_rows(
        s3, gold.KEY_PHRASE_DAILY_PREFIX, gold.KEY_PHRASE_DAILY_FILE
    )


ITEM_A = ("a", "Ecopetrol invierte en Medellín",
          "La empresa anunció obras en Medellín.", PUB_DATE)
ITEM_B = ("b", "Federico Gutiérrez habla en Medellín", "Rueda de prensa.",
          PUB_DATE)


# --- Limpieza y agregación ---------------------------------------------------

@pytest.mark.parametrize(
    "text, strip, expected",
    [
        ("estadio El Campín,", False, "estadio El Campín"),
        ("el gobernador Rendón", True, "gobernador Rendón"),
        ("el Balón de Oro el", True, "Balón de Oro"),
        ("¿Qué", True, "Qué"),
    ],
)
def test_clean_term(text, strip, expected):
    assert gold.clean_term(text, strip_determiners=strip) == expected


@pytest.mark.parametrize("key, expected", [
    ("medellín", True), ("2026", False), ("qué", False), ("los", False),
    ("", False),
])
def test_is_meaningful(key, expected):
    assert gold.is_meaningful(key) is expected


def news_row(news_id, sections=("Medellín",)):
    return {"news_id": news_id, "medium": "El Tiempo", "sections": list(sections)}


def entity_row(news_id, text, kind="LOCATION", score=0.9, mentions=1):
    return {
        "news_id": news_id, "entity_text": text, "entity_type": kind,
        "score": score, "mention_count": mentions,
    }


def phrase_row(news_id, text, score=0.95):
    return {"news_id": news_id, "key_phrase": text, "score": score}


def test_compute_partition_counts_distinct_news():
    news, entities, phrases = gold.compute_partition(
        [news_row("n1", ["Medellín", "Deportes"]), news_row("n2")],
        [
            entity_row("n1", "Medellín", mentions=3),
            entity_row("n1", "Medellín,"),       # misma entidad tras limpiar
            entity_row("n2", "MEDELLÍN", score=0.8),
            entity_row("n2", "420", score=0.39),  # bajo el umbral
            entity_row("huerfana", "Bogotá"),     # sin noticia asociada
        ],
        [
            phrase_row("n1", "el gobernador"),
            phrase_row("n2", "gobernador"),
            phrase_row("n2", "¿qué"),
            phrase_row("n2", "2026"),
        ],
    )

    assert news == [
        {"medium": "El Tiempo", "section": "Deportes", "news_count": 1},
        {"medium": "El Tiempo", "section": "Medellín", "news_count": 2},
    ]
    assert entities == [{
        "entity_type": "LOCATION",
        "entity": "medellín",
        "entity_text": "Medellín",
        "news_count": 2,
        "mention_count": 5,
        "max_score": 0.9,
    }]
    assert phrases == [
        {"section": "Deportes", "key_phrase": "gobernador",
         "key_phrase_text": "gobernador", "news_count": 1},
        {"section": "Medellín", "key_phrase": "gobernador",
         "key_phrase_text": "gobernador", "news_count": 2},
    ]


# --- Construcción incremental ------------------------------------------------

def test_initial_build_creates_all_tables(s3):
    run_silver(s3, "run-1", [ITEM_A, ITEM_B])

    summary = gold.build_gold("run-1")

    assert list(summary["partitions_rebuilt"]) == [DATE]
    assert [(row["section"], row["news_count"]) for row in news_daily(s3)] == [
        ("Medellín", 2),
    ]
    counts = {
        (row["entity_type"], row["entity_text"]): row["news_count"]
        for row in entity_daily(s3)
    }
    assert counts == {
        ("LOCATION", "Medellín"): 2,
        ("ORGANIZATION", "Ecopetrol"): 1,
        ("PERSON", "Federico Gutiérrez"): 1,
    }
    assert key_phrase_daily(s3)
    assert f"gold/_builds/run_id=run-1/summary.json" in s3.objects


def test_unchanged_silver_rebuilds_nothing(s3):
    run_silver(s3, "run-1", [ITEM_A, ITEM_B])
    gold.build_gold("run-1")

    # Repetir el lote no cambia Silver, así que Gold no se toca.
    run_silver(s3, "run-2", [ITEM_A, ITEM_B], hours=3)
    summary = gold.build_gold("run-2")

    assert summary["partitions_rebuilt"] == {}
    assert sum(row["news_count"] for row in news_daily(s3)) == 2


def test_modified_news_updates_gold(s3):
    run_silver(s3, "run-1", [ITEM_A, ITEM_B])
    gold.build_gold("run-1")

    changed = ("a", "Ecopetrol invierte en Bogotá", "Cambio de sede.", PUB_DATE)
    run_silver(s3, "run-2", [changed, ITEM_B], hours=3)
    summary = gold.build_gold("run-2")

    assert list(summary["partitions_rebuilt"]) == [DATE]
    counts = {row["entity_text"]: row["news_count"] for row in entity_daily(s3)}
    assert counts["Medellín"] == 1
    assert counts["Bogotá"] == 1


def test_moved_news_removes_old_gold_partition(s3):
    run_silver(s3, "run-1", [ITEM_A])
    gold.build_gold("run-1")

    moved = (
        "a", "Ecopetrol invierte en Bogotá", "Nueva fecha.",
        "Mon, 02 Nov 2026 01:00:00 -0500",
    )
    run_silver(s3, "run-2", [moved], hours=3)
    summary = gold.build_gold("run-2")

    assert sorted(summary["partitions_rebuilt"]) == [DATE, "2026-11-02"]
    assert {row["published_date"] for row in news_daily(s3)} == {"2026-11-02"}
    assert {row["published_date"] for row in entity_daily(s3)} == {"2026-11-02"}


def test_interrupted_build_is_completed_on_next_run(s3, monkeypatch):
    run_silver(s3, "run-1", [ITEM_A, ITEM_B])
    original = silver._write_table

    def fail_on_marker(key, rows, schema):
        if key.startswith(gold.NEWS_DAILY_PREFIX):
            raise RuntimeError("fallo simulado")
        return original(key, rows, schema)

    monkeypatch.setattr(silver, "_write_table", fail_on_marker)
    with pytest.raises(RuntimeError):
        gold.build_gold("run-1")
    assert entity_daily(s3) and not news_daily(s3)

    monkeypatch.setattr(silver, "_write_table", original)
    summary = gold.build_gold("run-2")

    assert list(summary["partitions_rebuilt"]) == [DATE]
    assert sum(row["news_count"] for row in news_daily(s3)) == 2


def test_gold_reconciles_with_silver(s3):
    run_silver(s3, "run-1", [ITEM_A, ITEM_B])
    run_silver(s3, "run-2", [("c", "Deportes en Bogotá", "", PUB_DATE)],
               hours=3, section="Deportes")
    gold.build_gold("run-2")

    silver_news = table(s3, silver.NEWS_PREFIX, silver.NEWS_FILE)
    expected = sum(len(row["sections"]) for row in silver_news)

    assert sum(row["news_count"] for row in news_daily(s3)) == expected == 3


def test_full_rebuild_rewrites_every_partition(s3):
    run_silver(s3, "run-1", [ITEM_A])
    gold.build_gold("run-1")

    summary = gold.build_gold("run-2", full=True)

    assert list(summary["partitions_rebuilt"]) == [DATE]
