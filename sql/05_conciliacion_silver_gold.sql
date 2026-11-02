-- Conciliación de Gold contra Silver.
--
-- Recalcula desde silver.news las noticias por día × medio × sección y las
-- compara con gold.news_daily. Devuelve solo las diferencias: un resultado
-- vacío significa que Gold está al día con Silver.
--
-- Una fila con silver_count NULL es una combinación que sobra en Gold; con
-- gold_count NULL, una que le falta (por ejemplo, una partición de Silver
-- que todavía no se ha reconstruido en Gold).
WITH silver_conteo AS (
    SELECT
        n.published_date,
        n.medium,
        s.section,
        count(*) AS silver_count
    FROM news n
    CROSS JOIN UNNEST(n.sections) AS s (section)
    GROUP BY n.published_date, n.medium, s.section
)
SELECT
    coalesce(s.published_date, g.published_date) AS published_date,
    coalesce(s.medium, g.medium)                 AS medium,
    coalesce(s.section, g.section)               AS section,
    s.silver_count,
    g.news_count                                 AS gold_count
FROM silver_conteo s
FULL OUTER JOIN news_daily g
    ON  g.published_date = s.published_date
    AND g.medium = s.medium
    AND g.section = s.section
WHERE s.silver_count IS NULL
   OR g.news_count IS NULL
   OR s.silver_count <> g.news_count
ORDER BY 1, 2, 3;
