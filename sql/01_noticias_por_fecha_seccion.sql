-- Pregunta 1: ¿cuántas noticias se publicaron por fecha, medio y sección?
--
-- Fuente: gold.news_daily (una fila por día × medio × sección).
-- Una noticia que aparece en varias secciones cuenta en cada una; por eso
-- el total de esta consulta puede ser mayor que el número de noticias
-- únicas de silver.news.
--
-- Para acotar el escaneo, agregue un filtro de partición, por ejemplo:
--   WHERE published_date BETWEEN '2026-10-26' AND '2026-11-01'
SELECT
    published_date,
    medium,
    section,
    news_count
FROM news_daily
ORDER BY published_date, medium, section;
