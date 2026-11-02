-- Pregunta 2: ¿cuáles son las 10 organizaciones mencionadas en más noticias?
--
-- Fuente: gold.entity_daily. news_count cuenta noticias distintas por día;
-- como cada noticia pertenece a una sola fecha (published_date), sumar los
-- días no cuenta dos veces la misma noticia.
--
-- Gold ya descarta entidades con score < 0.5. Limitación conocida de
-- Comprehend: equipos como "Santa Fe" o "Bucaramanga" salen como
-- ORGANIZATION aunque también son lugares.
SELECT
    entity                         AS entidad_normalizada,
    arbitrary(entity_text)         AS entidad,
    sum(news_count)                AS noticias,
    sum(mention_count)             AS menciones,
    count(*)                       AS dias_con_menciones,
    round(max(max_score), 3)       AS confianza_maxima
FROM entity_daily
WHERE entity_type = 'ORGANIZATION'
GROUP BY entity
ORDER BY noticias DESC, menciones DESC, entidad_normalizada
LIMIT 10;
