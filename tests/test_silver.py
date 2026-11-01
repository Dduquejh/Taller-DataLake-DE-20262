from datetime import datetime, timezone

import pytest

from src.pipeline.silver import (
    clean_text,
    collapse_batch,
    merge_batch,
    normalize_link,
    parse_items,
    parse_pub_date,
    validate_item,
)


FEED = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel>
  <title>Feed</title>
  <item>
    <title>Primera &lt;b&gt;noticia&lt;/b&gt;</title>
    <pubDate>2026-10-26T19:01:00-05:00</pubDate>
    <link>HTTPS://WWW.EJEMPLO.COM/a-1#arriba</link>
    <guid>https://www.ejemplo.com/a-1</guid>
    <description><![CDATA[<p>Texto   con <a href="#">HTML</a></p>]]></description>
  </item>
  <item>
    <title>   </title>
    <pubDate>no es una fecha</pubDate>
    <link>ftp://ejemplo.com/b</link>
    <guid>b</guid>
  </item>
</channel></rss>"""

SOURCE = {
    "source_id": "fuente_a",
    "medium": "El Tiempo",
    "section": "Medellín",
    "s3_key": "bronze/source=fuente_a/response.xml",
}

T1 = datetime(2026, 10, 27, 1, 0, tzinfo=timezone.utc)
T2 = datetime(2026, 10, 27, 4, 0, tzinfo=timezone.utc)


def make_record(
    news_id="n1",
    title="Título",
    fetched_at=T1,
    section="Medellín",
    source_id="fuente_a",
    published_at=datetime(2026, 10, 26, tzinfo=timezone.utc),
    run_id="run-1",
):
    from src.pipeline.silver import content_hash

    return {
        "news_id": news_id,
        "guid": news_id,
        "link": f"https://ejemplo.com/{news_id}",
        "title": title,
        "description": None,
        "medium": "El Tiempo",
        "section": section,
        "source_id": source_id,
        "pub_date_raw": published_at.isoformat(),
        "published_at": published_at,
        "content_hash": content_hash(title, None),
        "bronze_key": f"bronze/{run_id}/{source_id}.xml",
        "run_id": run_id,
        "fetched_at": fetched_at,
    }


def merge(state, records):
    batch, _ = collapse_batch(records)
    return merge_batch(state, batch, processed_at=T2)


def test_clean_text_strips_html_entities_and_whitespace():
    assert clean_text("Hola &lt;b&gt;mundo&lt;/b&gt;  \n ya") == "Hola mundo ya"
    assert clean_text("<p>Texto <a href='#'>HTML</a></p>") == "Texto HTML"
    assert clean_text("   ") is None
    assert clean_text(None) is None


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("2026-10-26T19:01:00-05:00", datetime(2026, 10, 27, 0, 1)),
        ("Mon, 26 Oct 2026 20:45:21 -0500", datetime(2026, 10, 27, 1, 45, 21)),
        ("2026-10-26T10:00:00Z", datetime(2026, 10, 26, 10, 0)),
    ],
)
def test_parse_pub_date_converts_to_utc(raw, expected):
    assert parse_pub_date(raw) == expected.replace(tzinfo=timezone.utc)


@pytest.mark.parametrize("raw", ["", "no es una fecha", "2026-10-26T10:00:00"])
def test_parse_pub_date_rejects_invalid_or_naive(raw):
    assert parse_pub_date(raw) is None


def test_normalize_link():
    assert (
        normalize_link(" HTTPS://WWW.Ejemplo.com/a?x=1#frag ")
        == "https://www.ejemplo.com/a?x=1"
    )
    assert normalize_link("ftp://ejemplo.com/a") is None
    assert normalize_link("no-es-url") is None


def test_parse_and_validate_items():
    items = parse_items(FEED)
    assert len(items) == 2

    record, reasons = validate_item(items[0], SOURCE, "run-1", T1)
    assert reasons == []
    assert record["title"] == "Primera noticia"
    assert record["description"] == "Texto con HTML"
    assert record["link"] == "https://www.ejemplo.com/a-1"
    assert record["published_at"] == datetime(
        2026, 10, 27, 0, 1, tzinfo=timezone.utc
    )
    assert record["pub_date_raw"] == "2026-10-26T19:01:00-05:00"
    assert record["bronze_key"] == SOURCE["s3_key"]

    record, reasons = validate_item(items[1], SOURCE, "run-1", T1)
    assert record is None
    assert reasons == ["titulo_vacio", "enlace_invalido", "fecha_invalida"]


def test_collapse_batch_merges_sections_of_same_news():
    batch, duplicates = collapse_batch([
        make_record(section="Medellín", source_id="fuente_a"),
        make_record(section="Deportes", source_id="fuente_b"),
        make_record(news_id="n2"),
    ])

    assert duplicates == 1
    assert len(batch) == 2
    assert batch[0]["sections"] == ["Deportes", "Medellín"]
    assert batch[0]["source_ids"] == ["fuente_a", "fuente_b"]


def test_merge_new_then_repeated_batch_is_idempotent():
    state = {}
    stats, dirty, _ = merge(state, [make_record()])
    assert stats["new"] == 1
    assert dirty == {"2026-10-26"}

    stats, dirty, _ = merge(state, [make_record()])
    assert stats["repeated"] == 1
    assert dirty == set()
    assert state["n1"]["version"] == 1


def test_merge_updated_content_creates_new_version():
    state = {}
    merge(state, [make_record()])

    stats, dirty, _ = merge(
        state,
        [make_record(title="Título corregido", fetched_at=T2, run_id="run-2")],
    )

    row = state["n1"]
    assert stats["updated"] == 1
    assert row["version"] == 2
    assert row["title"] == "Título corregido"
    assert row["first_run_id"] == "run-1"
    assert row["last_run_id"] == "run-2"
    assert dirty == {"2026-10-26"}


def test_reprocessing_older_batch_does_not_revert_update():
    state = {}
    merge(state, [make_record()])
    merge(state, [make_record(title="Nuevo", fetched_at=T2, run_id="run-2")])

    stats, dirty, _ = merge(state, [make_record()])

    assert stats["stale"] == 1
    assert dirty == set()
    assert state["n1"]["title"] == "Nuevo"


def test_update_that_moves_partition_marks_both_dates():
    state = {}
    merge(state, [make_record()])

    stats, dirty, gaining = merge(state, [make_record(
        title="Otro",
        fetched_at=T2,
        published_at=datetime(2026, 10, 27, tzinfo=timezone.utc),
    )])

    assert stats["updated"] == 1
    assert dirty == {"2026-10-26", "2026-10-27"}
    assert gaining == {"2026-10-27"}


def test_repeated_news_in_new_section_updates_sections_only():
    state = {}
    merge(state, [make_record()])

    stats, dirty, _ = merge(
        state,
        [make_record(section="Deportes", source_id="fuente_b", fetched_at=T2)],
    )

    assert stats["repeated"] == 1
    assert state["n1"]["sections"] == ["Deportes", "Medellín"]
    assert state["n1"]["version"] == 1
    assert dirty == {"2026-10-26"}
