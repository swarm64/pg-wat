DROP FUNCTION IF EXISTS scan_sizes(VARCHAR, VARCHAR);
CREATE FUNCTION scan_sizes(
    in_db_name VARCHAR
  , in_query_name VARCHAR
) RETURNS TABLE (
    node_type VARCHAR
  , custom_plan_provider VARCHAR
  , relation_name VARCHAR
  , total_scan_size BIGINT
  , total_scan_size_pretty TEXT
) AS $$
BEGIN
  RETURN QUERY
  WITH prefilter AS(
    SELECT
        s.node_type
      , s.custom_plan_provider
      , s.relation_name
      , (plan_width * actual_rows * COALESCE(parallel_workers, 1)) AS size
      , pg_size_pretty(plan_width * actual_rows * COALESCE(parallel_workers, 1)) AS size_pretty
    FROM pgwat.query_node_stats s
    WHERE db_name = in_db_name
      AND query_name = in_query_name
      AND ((s.node_type ILIKE '%scan' AND s.custom_plan_provider IS NULL)
        OR (s.node_type = 'Custom Scan' AND s.custom_plan_provider ILIKE '%scan'))
  )
  SELECT *
  FROM prefilter;
END;
$$ LANGUAGE plpgsql PARALLEL SAFE;

-- SELECT * FROM scan_sizes('tpch_sf100_pg', '5') ORDER BY total_scan_size;
-- SELECT * FROM scan_sizes('tpch_sf100_s64da', '5') ORDER BY total_scan_size;
