-- Pregunta 3: ¿cómo evoluciona la presencia de una entidad entre días?
--
-- Cambie el valor de la entidad en el CTE "objetivo". Se compara contra la
-- columna normalizada (minúsculas, sin puntuación), así que 'santa fe'
-- encuentra "Santa Fe".
--
-- Se listan todos los días con noticias, también aquellos en los que la
-- entidad no aparece (noticias = 0), y su participación sobre el total de
-- noticias del día.
WITH objetivo AS (
    SELECT 'santa fe' AS entity
),
dias AS (
    -- Noticias únicas por día (news_daily repite la noticia por sección).
    SELECT published_date, count(*) AS noticias_del_dia
    FROM news
    GROUP BY published_date
),
entidad AS (
    SELECT
        e.published_date,
        sum(e.news_count)    AS noticias,
        sum(e.mention_count) AS menciones
    FROM entity_daily e
    JOIN objetivo o ON e.entity = o.entity
    GROUP BY e.published_date
)
SELECT
    d.published_date,
    coalesce(en.noticias, 0)  AS noticias,
    coalesce(en.menciones, 0) AS menciones,
    d.noticias_del_dia,
    round(100.0 * coalesce(en.noticias, 0) / d.noticias_del_dia, 1) AS pct_noticias_del_dia
FROM dias d
LEFT JOIN entidad en ON en.published_date = d.published_date
ORDER BY d.published_date;
