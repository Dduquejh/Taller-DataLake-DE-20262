"""
Enriquecimiento con Amazon Comprehend: entidades y frases clave.

Usa las llamadas síncronas por lotes; el texto viaja en la solicitud,
por lo que Comprehend no necesita acceso al bucket.
"""

import re
from collections import Counter, defaultdict

import boto3
from botocore.config import Config

from src.pipeline.ingestion import REGION, utc_now


# Límite de las llamadas Batch* por documento.
MAX_TEXT_BYTES = 5000
BATCH_SIZE = 25

# Idiomas que el pipeline analiza; Comprehend admite más, pero las
# fuentes son en español y algunos titulares de tecnología en inglés.
CONTEMPLATED_LANGUAGES = {"es", "en"}

ENTITY_TYPES = {"PERSON", "ORGANIZATION", "LOCATION"}

# Estados que no requieren un nuevo análisis mientras el texto no cambie.
FINAL_STATUSES = {
    "analyzed",
    "empty_text",
    "too_long",
    "unsupported_language",
}

WHITESPACE = re.compile(r"\s+")

# Los reintentos estándar cubren la limitación de tasa de Comprehend.
comprehend = boto3.Session(region_name=REGION).client(
    "comprehend",
    config=Config(retries={"max_attempts": 5, "mode": "standard"}),
)


def _chunks(items, size=BATCH_SIZE):
    for start in range(0, len(items), size):
        yield items[start:start + size]


def _clean_mention(text):
    return WHITESPACE.sub(" ", text).strip()


def distinct_mentions(mentions, types=None):
    """
    Agrupa las menciones de un texto en elementos distintos.

    Dos menciones son la misma si coinciden en tipo y en texto sin
    distinguir mayúsculas. Se conserva la primera forma escrita, la
    mayor puntuación y la cantidad de apariciones.
    """
    grouped = {}

    for mention in mentions:
        mention_type = mention.get("Type")

        if types is not None and mention_type not in types:
            continue

        text = _clean_mention(mention["Text"])

        if not text:
            continue

        key = (mention_type, text.casefold())
        item = grouped.get(key)

        if item is None:
            grouped[key] = {
                "text": text,
                "normalized": text.casefold(),
                "type": mention_type,
                "score": float(mention["Score"]),
                "mention_count": 1,
            }
            continue

        item["score"] = max(item["score"], float(mention["Score"]))
        item["mention_count"] += 1

    return sorted(
        grouped.values(),
        key=lambda item: (item["type"] or "", item["normalized"]),
    )


def _result(text, status, analyzed_at, **fields):
    return {
        "status": status,
        "text": text,
        "language": None,
        "language_score": None,
        "analyzed_at": analyzed_at,
        "error": None,
        "entities": [],
        "key_phrases": [],
        **fields,
    }


def _errors_by_index(response):
    return {
        error["Index"]: f"{error['ErrorCode']}: {error['ErrorMessage']}"
        for error in response.get("ErrorList", [])
    }


def analyze_texts(texts):
    """
    Analiza textos identificados por su hash.

    texts: {hash: texto}. Devuelve ({hash: resultado}, conteos).
    Cada resultado tiene un estado; solo los textos válidos en un idioma
    contemplado se envían a la detección de entidades y frases clave.
    """
    analyzed_at = utc_now()
    results = {}
    calls = Counter()
    pending = []

    for text_hash, text in texts.items():
        if not text or not text.strip():
            results[text_hash] = _result(text, "empty_text", analyzed_at)
        elif len(text.encode("utf-8")) > MAX_TEXT_BYTES:
            results[text_hash] = _result(text, "too_long", analyzed_at)
        else:
            pending.append((text_hash, text))

    by_language = defaultdict(list)

    for chunk in _chunks(pending):
        response = comprehend.batch_detect_dominant_language(
            TextList=[text for _, text in chunk]
        )
        calls["detect_dominant_language"] += 1
        errors = _errors_by_index(response)

        for item in response.get("ResultList", []):
            text_hash, text = chunk[item["Index"]]

            if not item.get("Languages"):
                results[text_hash] = _result(
                    text,
                    "unsupported_language",
                    analyzed_at,
                    error="Comprehend no detectó un idioma.",
                )
                continue

            best = max(item["Languages"], key=lambda lang: lang["Score"])
            language = best["LanguageCode"]
            score = float(best["Score"])

            if language not in CONTEMPLATED_LANGUAGES:
                results[text_hash] = _result(
                    text,
                    "unsupported_language",
                    analyzed_at,
                    language=language,
                    language_score=score,
                )
                continue

            by_language[language].append((text_hash, text, score))

        for index, error in errors.items():
            text_hash, text = chunk[index]
            results[text_hash] = _result(
                text, "error", analyzed_at, error=error
            )

    for language, items in by_language.items():
        for chunk in _chunks(items):
            text_list = [text for _, text, _ in chunk]

            entities = comprehend.batch_detect_entities(
                TextList=text_list,
                LanguageCode=language,
            )
            phrases = comprehend.batch_detect_key_phrases(
                TextList=text_list,
                LanguageCode=language,
            )
            calls["detect_entities"] += 1
            calls["detect_key_phrases"] += 1

            entities_by_index = {
                item["Index"]: item["Entities"]
                for item in entities.get("ResultList", [])
            }
            phrases_by_index = {
                item["Index"]: item["KeyPhrases"]
                for item in phrases.get("ResultList", [])
            }
            errors = {**_errors_by_index(entities), **_errors_by_index(phrases)}

            for index, (text_hash, text, score) in enumerate(chunk):
                if index in errors:
                    results[text_hash] = _result(
                        text,
                        "error",
                        analyzed_at,
                        language=language,
                        language_score=score,
                        error=errors[index],
                    )
                    continue

                results[text_hash] = _result(
                    text,
                    "analyzed",
                    analyzed_at,
                    language=language,
                    language_score=score,
                    entities=distinct_mentions(
                        entities_by_index.get(index, []),
                        types=ENTITY_TYPES,
                    ),
                    key_phrases=[
                        {key: value for key, value in phrase.items()
                         if key != "type"}
                        for phrase in distinct_mentions(
                            phrases_by_index.get(index, [])
                        )
                    ],
                )

    statuses = Counter(result["status"] for result in results.values())
    return results, {"api_calls": dict(calls), "statuses": dict(statuses)}
