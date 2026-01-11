-- 📊 QUERY DE ANÁLISIS DE ESPACIO SUPABASE
-- Ejecuta esto en el SQL Editor de Supabase dashboard

WITH table_sizes AS (
    SELECT
        relname AS table_name,
        pg_total_relation_size(relid) AS total_bytes,
        pg_relation_size(relid) AS data_bytes,
        (pg_total_relation_size(relid) - pg_relation_size(relid)) AS index_bytes,
        reltuples::bigint AS estimated_rows
    FROM pg_catalog.pg_stat_user_tables
    WHERE schemaname = 'public'
)
SELECT
    table_name,
    pg_size_pretty(total_bytes) AS total_size_pretty,
    pg_size_pretty(data_bytes) AS data_size_pretty,
    pg_size_pretty(index_bytes) AS index_size_pretty,
    estimated_rows,
    -- Estimar bytes por fila (útil para proyecciones)
    CASE WHEN estimated_rows > 0 
         THEN (total_bytes / estimated_rows) 
         ELSE 0 
    END AS bytes_per_row_estimate
FROM table_sizes
ORDER BY total_bytes DESC;

-- 📦 TAMAÑO TOTAL DE LA BASE DE DATOS
SELECT 
    pg_size_pretty(pg_database_size(current_database())) as current_db_usage,
    '500 MB' as free_tier_limit,
    ROUND((pg_database_size(current_database()) / (500.0 * 1024 * 1024)) * 100, 2) || '%' as usage_percentage;
