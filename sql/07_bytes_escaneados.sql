-- Efecto del filtro de partición en los bytes escaneados.
--
-- Las dos consultas calculan lo mismo; la segunda filtra por la columna de
-- partición published_date, así que Athena solo lee esa carpeta de S3.
-- Compare "Data scanned" en la consola de Athena, o ejecute:
--   python scripts/run_athena.py sql/07_bytes_escaneados.sql
-- que imprime los bytes escaneados de cada consulta.
--
-- Athena cobra por bytes escaneados (mínimo 10 MB por consulta), así que
-- con el volumen actual el costo es el mismo; la diferencia crece con el
-- histórico.

-- Sin filtro de partición: lee todas las fechas.
SELECT count(*) AS noticias, max(length(description)) AS descripcion_mas_larga
FROM news;

-- Con filtro de partición: lee una sola fecha.
SELECT count(*) AS noticias, max(length(description)) AS descripcion_mas_larga
FROM news
WHERE published_date = '2026-10-26';
