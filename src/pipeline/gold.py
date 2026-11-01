"""
Capa Gold: agregados de Silver para responder las preguntas del negocio.

Tablas, particionadas por published_date (la fecha de publicación en UTC):

- news_daily: noticias por medio y sección.
- entity_daily: noticias distintas en que aparece cada entidad.
- key_phrase_daily: noticias distintas en que aparece cada frase clave,
  por sección.

Todas las métricas cuentan noticias distintas: una entidad repetida dentro
de un texto suma una sola vez. Cada noticia pertenece a una sola fecha, por
lo que sumar los conteos diarios da el total de noticias distintas.

Una partición se reconstruye si su versión de Silver es más reciente que la
de Gold, si falta en Gold o si ya no existe en Silver. news_daily se escribe
al final y marca la partición como completa.
"""

import argparse
import io
import json
from collections import Counter, defaultdict

import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

from src.pipeline import silver
from src.pipeline.ingestion import BUCKET, _is_not_found, get_object, s3, utc_now


NEWS_DAILY_PREFIX = "gold/news_daily/"
NEWS_DAILY_FILE = "news_daily.parquet"
ENTITY_DAILY_PREFIX = "gold/entity_daily/"
ENTITY_DAILY_FILE = "entity_daily.parquet"
KEY_PHRASE_DAILY_PREFIX = "gold/key_phrase_daily/"
KEY_PHRASE_DAILY_FILE = "key_phrase_daily.parquet"
BUILDS_PREFIX = "gold/_builds/"

# Por debajo de estos valores, Comprehend acierta poco en estos textos cortos
# (por ejemplo "de" como lugar o "420" como lugar).
MIN_ENTITY_SCORE = 0.5
MIN_KEY_PHRASE_SCORE = 0.8

PUNCTUATION = " \t\n.,;:¿?¡!\"'«»“”‘’()[]{}-–—…"

# Palabras que no aportan al inicio o al final de una frase clave.
DETERMINERS = {
    "el", "la", "los", "las", "lo", "un", "una", "unos", "unas",
    "su", "sus", "este", "esta", "estos", "estas", "ese", "esa", "esos",
    "esas", "del", "al", "de", "y", "o", "en", "con", "por", "para",
}

QUESTION_WORDS = {
    "qué", "quién", "quiénes", "cómo", "cuál", "cuáles", "dónde",
    "cuándo", "cuánto", "cuánta", "cuántos", "cuántas", "por qué",
}

NEWS_DAILY_SCHEMA = pa.schema([
    ("medium", pa.string()),
    ("section", pa.string()),
    ("news_count", pa.int64()),
])

ENTITY_DAILY_SCHEMA = pa.schema([
    ("entity_type", pa.string()),
    ("entity", pa.string()),
    ("entity_text", pa.string()),
    ("news_count", pa.int64()),
    ("mention_count", pa.int64()),
    ("max_score", pa.float64()),
])

KEY_PHRASE_DAILY_SCHEMA = pa.schema([
    ("section", pa.string()),
    ("key_phrase", pa.string()),
    ("key_phrase_text", pa.string()),
    ("news_count", pa.int64()),
])


def clean_term(text, strip_determiners=False):
    """
    Limpia una entidad o frase para agruparla.

    Quita signos de puntuación de cada palabra y, en las frases clave,
    artículos y preposiciones al inicio y al final. Conserva mayúsculas;
    la clave de agrupación es el resultado en minúsculas.
    """
    words = [word.strip(PUNCTUATION) for word in text.split()]
    words = [word for word in words if word]

    if strip_determiners:
        while words and words[0].casefold() in DETERMINERS:
            words.pop(0)
        while words and words[-1].casefold() in DETERMINERS:
            words.pop()

    return " ".join(words)


def is_meaningful(key):
    """Descarta términos vacíos, solo numéricos, artículos o interrogativos."""
    return (
        any(char.isalpha() for char in key)
        and key not in DETERMINERS
        and key not in QUESTION_WORDS
    )


def _display(forms):
    """Forma escrita más frecuente; en empate, la primera alfabéticamente."""
    return min(forms.items(), key=lambda item: (-item[1], item[0]))[0]


def compute_partition(news, entities, key_phrases):
    """
    Calcula los agregados Gold de una fecha a partir de sus filas de Silver.

    Devuelve (news_daily, entity_daily, key_phrase_daily).
    """
    news_by_id = {row["news_id"]: row for row in news}

    sections_count = Counter()
    for row in news:
        for section in row["sections"]:
            sections_count[(row["medium"], section)] += 1

    news_daily = [
        {"medium": medium, "section": section, "news_count": count}
        for (medium, section), count in sorted(sections_count.items())
    ]

    entity_groups = defaultdict(lambda: {
        "news": set(), "mentions": 0, "score": 0.0, "forms": Counter(),
    })

    for row in entities:
        if row["news_id"] not in news_by_id or row["score"] < MIN_ENTITY_SCORE:
            continue

        text = clean_term(row["entity_text"])
        key = text.casefold()

        if not is_meaningful(key):
            continue

        group = entity_groups[(row["entity_type"], key)]
        group["news"].add(row["news_id"])
        group["mentions"] += row["mention_count"]
        group["score"] = max(group["score"], row["score"])
        group["forms"][text] += 1

    entity_daily = [
        {
            "entity_type": entity_type,
            "entity": key,
            "entity_text": _display(group["forms"]),
            "news_count": len(group["news"]),
            "mention_count": group["mentions"],
            "max_score": group["score"],
        }
        for (entity_type, key), group in sorted(entity_groups.items())
    ]

    phrase_groups = defaultdict(lambda: {"news": set(), "forms": Counter()})

    for row in key_phrases:
        owner = news_by_id.get(row["news_id"])

        if owner is None or row["score"] < MIN_KEY_PHRASE_SCORE:
            continue

        text = clean_term(row["key_phrase"], strip_determiners=True)
        key = text.casefold()

        if not is_meaningful(key):
            continue

        for section in owner["sections"]:
            group = phrase_groups[(section, key)]
            group["news"].add(row["news_id"])
            group["forms"][text] += 1

    key_phrase_daily = [
        {
            "section": section,
            "key_phrase": key,
            "key_phrase_text": _display(group["forms"]),
            "news_count": len(group["news"]),
        }
        for (section, key), group in sorted(phrase_groups.items())
    ]

    return news_daily, entity_daily, key_phrase_daily


def _last_modified_by_date(prefixes):
    """Fecha de modificación más reciente por partición en esas tablas."""
    latest = {}
    paginator = s3.get_paginator("list_objects_v2")

    for prefix in prefixes:
        for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                if "published_date=" not in obj["Key"]:
                    continue

                day = silver._partition_from_key(obj["Key"])
                modified = obj["LastModified"]

                if day not in latest or modified > latest[day]:
                    latest[day] = modified

    return latest


def _gold_markers():
    """Fecha de modificación del archivo que marca cada partición completa."""
    markers = {}
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=BUCKET, Prefix=NEWS_DAILY_PREFIX):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(f"/{NEWS_DAILY_FILE}"):
                markers[silver._partition_from_key(obj["Key"])] = (
                    obj["LastModified"]
                )

    return markers


def stale_partitions():
    """Particiones de Gold que no reflejan la versión actual de Silver."""
    silver_dates = _last_modified_by_date([
        silver.NEWS_PREFIX,
        silver.ENTITIES_PREFIX,
        silver.KEY_PHRASES_PREFIX,
    ])
    gold_dates = _last_modified_by_date([
        NEWS_DAILY_PREFIX,
        ENTITY_DAILY_PREFIX,
        KEY_PHRASE_DAILY_PREFIX,
    ])
    markers = _gold_markers()

    stale = {
        day
        for day, modified in silver_dates.items()
        if day not in markers or modified > markers[day]
    }

    # Particiones que ya no existen en Silver: se eliminan de Gold.
    stale |= set(gold_dates) - set(silver_dates)
    return sorted(stale)


def _read_silver(prefix, file_name, day):
    try:
        body = get_object(f"{prefix}published_date={day}/{file_name}")
    except ClientError as exc:
        if _is_not_found(exc):
            return []
        raise

    return pq.read_table(io.BytesIO(body)).to_pylist()


# news_daily va al final: su presencia marca la partición como completa.
TABLES = [
    (ENTITY_DAILY_PREFIX, ENTITY_DAILY_FILE, ENTITY_DAILY_SCHEMA, 1),
    (KEY_PHRASE_DAILY_PREFIX, KEY_PHRASE_DAILY_FILE, KEY_PHRASE_DAILY_SCHEMA, 2),
    (NEWS_DAILY_PREFIX, NEWS_DAILY_FILE, NEWS_DAILY_SCHEMA, 0),
]


def build_partition(day):
    """Reconstruye las tablas Gold de una fecha. Devuelve filas por tabla."""
    results = compute_partition(
        _read_silver(silver.NEWS_PREFIX, silver.NEWS_FILE, day),
        _read_silver(silver.ENTITIES_PREFIX, silver.ENTITIES_FILE, day),
        _read_silver(silver.KEY_PHRASES_PREFIX, silver.KEY_PHRASES_FILE, day),
    )
    counts = {}

    for prefix, file_name, schema, index in TABLES:
        silver._write_table(
            f"{prefix}published_date={day}/{file_name}",
            results[index],
            schema,
        )
        counts[file_name.removesuffix(".parquet")] = len(results[index])

    return counts


def build_gold(run_id=None, full=False):
    """
    Actualiza Gold desde Silver.

    Por defecto solo reconstruye las particiones desactualizadas; con
    full=True reconstruye todas las que existen en Silver o en Gold.
    """
    started_at = utc_now()

    if full:
        days = sorted(
            set(_last_modified_by_date([silver.NEWS_PREFIX]))
            | set(_last_modified_by_date([
                NEWS_DAILY_PREFIX,
                ENTITY_DAILY_PREFIX,
                KEY_PHRASE_DAILY_PREFIX,
            ]))
        )
    else:
        days = stale_partitions()

    partitions = {}

    for day in days:
        partitions[day] = build_partition(day)
        print(f"GOLD | {run_id} | {day} | {partitions[day]}")

    summary = {
        "run_id": run_id,
        "started_at": started_at.isoformat(),
        "finished_at": utc_now().isoformat(),
        "full_rebuild": full,
        "min_entity_score": MIN_ENTITY_SCORE,
        "min_key_phrase_score": MIN_KEY_PHRASE_SCORE,
        "partitions_rebuilt": partitions,
    }

    if run_id:
        silver._put_json(
            f"{BUILDS_PREFIX}run_id={run_id}/summary.json",
            json.dumps(summary, ensure_ascii=False, indent=2),
        )

    print(
        f"GOLD | {run_id} | particiones reconstruidas={len(partitions)} "
        f"{sorted(partitions)}"
    )

    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Actualiza Gold desde Silver.")
    parser.add_argument("--run-id")
    parser.add_argument(
        "--full",
        action="store_true",
        help="Reconstruir todas las particiones.",
    )
    args = parser.parse_args()

    build_gold(args.run_id, args.full)
