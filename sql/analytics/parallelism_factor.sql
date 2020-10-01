DROP FUNCTION IF EXISTS parallelism_factor(VARCHAR, VARCHAR);
CREATE FUNCTION parallelism_factor(
    in_db_name VARCHAR
  , in_query_name VARCHAR
) RETURNS NUMERIC AS $$
DECLARE
  parallelism_factor NUMERIC;
BEGIN
  WITH prefilter AS(
    SELECT parallel_workers
    FROM pgwat.query_node_stats
    WHERE db_name = in_db_name
      AND query_name = in_query_name
  )
  SELECT COUNT(*)::NUMERIC / (SELECT COUNT(*)::NUMERIC FROM prefilter)
  FROM prefilter
  WHERE parallel_workers > 0
  INTO parallelism_factor;

  RETURN ROUND(parallelism_factor * 100);
END;
$$ LANGUAGE plpgsql PARALLEL SAFE;


-- SELECT * FROM parallelism_factor('tpch_sf100_pg', '18');
-- SELECT * FROM parallelism_factor('tpch_sf100_s64da', '18');
--
-- WITH parallelism_factors AS (
--   SELECT db_name, query_name, parallelism_factor(db_name, query_name) AS factor
--   FROM pgwat.query_node_stats
--   GROUP BY 1, 2
-- )
-- SELECT
--     a.query_name
--   , a.factor AS factor_s64da
--   , b.factor AS factor_pg
-- FROM parallelism_factors a
-- LEFT JOIN parallelism_factors b ON a.query_name = b.query_name AND b.db_name LIKE '%pg'
-- WHERE a.db_name LIKE '%s64da'
-- ORDER BY 3 ASC
-- ;
