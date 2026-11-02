-- Pregunta 4: ¿cuáles son las frases clave más frecuentes por sección?
--
-- Fuente: gold.key_phrase_daily. Gold ya descarta frases con score < 0.8,
-- quita artículos al inicio y al final, y descarta términos solo numéricos
-- e interrogativos. Se muestran las 5 frases presentes en más noticias de
-- cada sección.
WITH por_seccion AS (
    SELECT
        section,
        key_phrase,
        arbitrary(key_phrase_text) AS frase,
        sum(news_count)            AS noticias
    FROM key_phrase_daily
    GROUP BY section, key_phrase
),
ranking AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY section
            ORDER BY noticias DESC, key_phrase
        ) AS puesto
    FROM por_seccion
)
SELECT section, puesto, frase, noticias
FROM ranking
WHERE puesto <= 5
ORDER BY section, puesto;
