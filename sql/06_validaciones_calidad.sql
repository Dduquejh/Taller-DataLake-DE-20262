-- Validaciones de calidad sobre Silver y Gold.
--
-- Cada fila es una regla y "failures" el número de registros que la
-- incumplen. Todas deben dar 0. La tarea de validación final del DAG puede
-- ejecutar esta consulta y fallar si alguna fila es mayor que 0.
SELECT 'news_id duplicado en silver.news' AS check_name,
       count(*) - count(DISTINCT news_id) AS failures
FROM news

UNION ALL
SELECT 'campos obligatorios vacíos (news_id, title, link, published_at)',
       count_if(
           nullif(trim(news_id), '') IS NULL
           OR nullif(trim(title), '') IS NULL
           OR nullif(trim(link), '') IS NULL
           OR published_at IS NULL
       )
FROM news

UNION ALL
SELECT 'enlace que no es http(s)',
       count_if(NOT regexp_like(link, '^https?://[^\s/]+\.[^\s]+$'))
FROM news

UNION ALL
SELECT 'fecha de publicación distinta de la partición',
       count_if(format_datetime(published_at, 'yyyy-MM-dd') <> published_date)
FROM news

UNION ALL
SELECT 'fecha de publicación en el futuro (más de 1 día)',
       count_if(published_at > current_timestamp + INTERVAL '1' DAY)
FROM news

UNION ALL
SELECT 'news_entities sin noticia en silver.news',
       count(*)
FROM news_entities e
LEFT JOIN news n
    ON n.news_id = e.news_id AND n.published_date = e.published_date
WHERE n.news_id IS NULL

UNION ALL
SELECT 'news_key_phrases sin noticia en silver.news',
       count(*)
FROM news_key_phrases k
LEFT JOIN news n
    ON n.news_id = k.news_id AND n.published_date = k.published_date
WHERE n.news_id IS NULL

UNION ALL
SELECT 'entidad duplicada en una noticia',
       count(*) - count(DISTINCT (news_id, entity_type, entity_text_normalized))
FROM news_entities

UNION ALL
SELECT 'diferencias entre gold.news_daily y silver.news',
       count(*)
FROM (
    SELECT n.published_date, n.medium, s.section, count(*) AS silver_count
    FROM news n
    CROSS JOIN UNNEST(n.sections) AS s (section)
    GROUP BY n.published_date, n.medium, s.section
) s
FULL OUTER JOIN news_daily g
    ON  g.published_date = s.published_date
    AND g.medium = s.medium
    AND g.section = s.section
WHERE s.silver_count IS NULL
   OR g.news_count IS NULL
   OR s.silver_count <> g.news_count;
