import io
from collections import Counter
from datetime import datetime, timedelta, timezone

from botocore.exceptions import ClientError


def client_error(code, status):
    return ClientError(
        {
            "Error": {"Code": code},
            "ResponseMetadata": {"HTTPStatusCode": status},
        },
        "S3",
    )


class FakeS3:
    """S3 en memoria con las operaciones y escrituras condicionales usadas."""

    def __init__(self):
        self.objects = {}
        self.versions = 0
        self.modified = {}

    def _etag(self, key):
        return self.objects[key][1]

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise client_error("NoSuchKey", 404)

        body, etag = self.objects[Key]
        return {"Body": io.BytesIO(body), "ETag": etag}

    def head_object(self, Bucket, Key):
        if Key not in self.objects:
            raise client_error("404", 404)

        return {"ETag": self._etag(Key)}

    def put_object(self, Bucket, Key, Body, IfNoneMatch=None, IfMatch=None,
                   **kwargs):
        if IfNoneMatch == "*" and Key in self.objects:
            raise client_error("PreconditionFailed", 412)

        if IfMatch is not None and (
            Key not in self.objects or self._etag(Key) != IfMatch
        ):
            raise client_error("PreconditionFailed", 412)

        self.versions += 1
        self.objects[Key] = (Body, f'"v{self.versions}"')
        # Cada escritura es un segundo posterior a la anterior.
        self.modified[Key] = datetime(2026, 1, 1, tzinfo=timezone.utc) + (
            timedelta(seconds=self.versions)
        )
        return {"ETag": self._etag(Key)}

    def delete_object(self, Bucket, Key):
        self.objects.pop(Key, None)

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return self

    def paginate(self, Bucket, Prefix):
        keys = sorted(key for key in self.objects if key.startswith(Prefix))
        yield {
            "Contents": [
                {"Key": key, "LastModified": self.modified.get(key)}
                for key in keys
            ]
        }


# Entidades que el Comprehend simulado reconoce en cualquier texto.
KNOWN_ENTITIES = {
    "Federico Gutiérrez": "PERSON",
    "Ecopetrol": "ORGANIZATION",
    "Medellín": "LOCATION",
    "Bogotá": "LOCATION",
    "2026": "DATE",
}


class FakeComprehend:
    """
    Comprehend simulado y determinista.

    - "Hello" se detecta en inglés y "Bonjour" en francés; lo demás en español.
    - "FALLA" provoca un error de documento en la detección de entidades.
    - La frase clave es la primera línea del texto (el título).
    """

    def __init__(self):
        self.calls = Counter()
        self.sent = []

    def batch_detect_dominant_language(self, TextList):
        assert len(TextList) <= 25
        self.calls["detect_dominant_language"] += 1
        results = []

        for index, text in enumerate(TextList):
            language = "es"
            if "Hello" in text:
                language = "en"
            elif "Bonjour" in text:
                language = "fr"

            results.append({
                "Index": index,
                "Languages": [{"LanguageCode": language, "Score": 0.99}],
            })

        return {"ResultList": results, "ErrorList": []}

    def batch_detect_entities(self, TextList, LanguageCode):
        assert len(TextList) <= 25
        self.calls["detect_entities"] += 1
        self.sent.extend(TextList)
        results = []
        errors = []

        for index, text in enumerate(TextList):
            if "FALLA" in text:
                errors.append({
                    "Index": index,
                    "ErrorCode": "InternalServerException",
                    "ErrorMessage": "falla simulada",
                })
                continue

            entities = [
                {"Text": name, "Type": kind, "Score": 0.9}
                for name, kind in KNOWN_ENTITIES.items()
                for _ in range(text.count(name))
            ]
            results.append({"Index": index, "Entities": entities})

        return {"ResultList": results, "ErrorList": errors}

    def batch_detect_key_phrases(self, TextList, LanguageCode):
        assert len(TextList) <= 25
        self.calls["detect_key_phrases"] += 1

        return {
            "ResultList": [
                {
                    "Index": index,
                    "KeyPhrases": [
                        {"Text": text.split("\n")[0], "Score": 0.95}
                    ],
                }
                for index, text in enumerate(TextList)
            ],
            "ErrorList": [],
        }
