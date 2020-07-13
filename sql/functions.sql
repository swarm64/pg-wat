DROP FUNCTION IF EXISTS explain_wrapper(TEXT, BOOL);
CREATE FUNCTION explain_wrapper(sql_query TEXT, run_analyze BOOL) RETURNS TABLE(plan JSON) AS
$BODY$
BEGIN
  IF run_analyze THEN
    RETURN QUERY EXECUTE 'EXPLAIN (ANALYZE, FORMAT JSON) ' || sql_query;
  ELSE
    RETURN QUERY EXECUTE 'EXPLAIN (FORMAT JSON) ' || sql_query;
  END IF;
END;
$BODY$ LANGUAGE plpgsql PARALLEL SAFE;

-- DROP FUNCTION IF EXISTS analyze_query(TEXT);
-- CREATE FUNCTION analyze_query(sql_query TEXT) RETURNS VOID AS
-- $BODY$
-- BEGIN
--   -- CREATE TEMP TABLE exp AS SELECT * FROM explain_wrapper(sql_query);

DROP FUNCTION IF EXISTS first_occurence_not_null(INT[]);
CREATE FUNCTION first_occurence_not_null(arr INT[]) RETURNS INT AS
$BODY$
DECLARE
  item RECORD;
BEGIN
  FOR item IN (SELECT elem, pos FROM UNNEST(arr) WITH ORDINALITY AS t(elem, pos))
  LOOP
    IF item.elem IS NOT NULL THEN
      RETURN item.pos;
    END IF;
  END LOOP;
  RETURN NULL;
END;
$BODY$ LANGUAGE plpgsql PARALLEL SAFE;

DROP FUNCTION IF EXISTS parse_explain_plan(JSON);
CREATE FUNCTION parse_explain_plan(in_plan JSON) RETURNS VOID AS
$BODY$
DECLARE
  plan_uuid UUID;
BEGIN
  DROP TABLE IF EXISTS local_query_node_stats;
  CREATE TEMP TABLE local_query_node_stats AS
  WITH RECURSIVE t(subplans, parallel_workers_arr) AS (
    SELECT
        subplans.* AS subplans
      , '{NULL}'::INT[] AS parallel_workers_arr
    FROM json_array_elements((SELECT in_plan->0->'Plan'->'Plans')) subplans
    UNION ALL
    SELECT
        subplans
      , parallel_workers_arr
    FROM (
      SELECT
          json_array_elements((SELECT subplans->'Plans')) AS subplans
        , array_append(parallel_workers_arr, COALESCE(
              (subplans->>'Workers Launched')::INT
            , (subplans->>'Workers Planned')::INT
          )) AS parallel_workers_arr
      FROM t
    ) a
  )
  SELECT
      (rec).* AS subplans
    , COALESCE(
          (rec)."Workers Launched"
        , (rec)."Workers Planned"
        , (SELECT max(parallel_workers) FROM unnest(parallel_workers_arr) AS parallel_workers)
      ) AS "Parallel Workers"
    , first_occurence_not_null(parallel_workers_arr) AS "Gather Node Depth"
  FROM (
    SELECT
        json_populate_record(null::exp_type, in_plan->0->'Plan') AS rec
      , '{NULL}'::INT[] AS parallel_workers_arr
    UNION ALL
    SELECT
        json_populate_record(null::exp_type, subplans) AS rec
      , parallel_workers_arr
    FROM t
  ) b;

  DROP SEQUENCE IF EXISTS query_node_stats_seq;
  CREATE TEMP SEQUENCE query_node_stats_seq;

  ALTER TABLE local_query_node_stats ADD COLUMN id INTEGER DEFAULT nextval('query_node_stats_seq');
  ALTER TABLE local_query_node_stats ADD COLUMN "Own Cost" DOUBLE PRECISION DEFAULT NULL;
  ALTER TABLE local_query_node_stats ADD COLUMN "Own Time" DOUBLE PRECISION DEFAULT NULL;

  -- WARNING: does not care about init plans atm
  WITH own AS(
    SELECT
        id
      , CASE WHEN own_cost = 0 AND is_shuffle THEN child_cost
             WHEN own_cost < 0 THEN 0
             ELSE own_cost
        END AS own_cost
      , own_time
    FROM (
      SELECT
          id
        , ROUND(COALESCE("Total Cost" - x.child_cost, "Total Cost")::NUMERIC, 2) AS own_cost
        , ROUND(COALESCE(
              "Actual Total Time" * CASE WHEN "Parallel Aware" THEN 1 ELSE "Actual Loops" END - x.child_time
            , "Actual Total Time"
          )::NUMERIC, 2) AS own_time
        , x.child_cost AS child_cost
        , "Custom Plan Provider" ILIKE '%s64%shuffle%' AS is_shuffle
      FROM local_query_node_stats
      LEFT JOIN LATERAL(
        SELECT
            SUM("Total Cost") AS child_cost
          , SUM("Actual Total Time" * CASE WHEN "Parallel Aware" THEN 1 ELSE "Actual Loops" END) AS child_time
        FROM json_populate_recordset(null::exp_type, local_query_node_stats."Plans")
      ) x ON true
    ) cost_calc
  ) UPDATE local_query_node_stats
  SET "Own Cost" = own.own_cost
    , "Own Time" = own.own_time
  FROM own
  WHERE local_query_node_stats.id = own.id;

  INSERT INTO query_node_stats
  SELECT
      nextval('query_node_stats_id_seq'::regclass)
    , (SELECT uuid_in(md5(random()::text || clock_timestamp()::text)::cstring))
    , id
    , "Node Type"
    , "Custom Plan Provider"
    , "Partial Mode"
    , "Relation Name"
    , "Startup Cost"
    , "Total Cost"
    , "Own Cost"
    , "Actual Startup Time"
    , "Actual Total Time"
    , "Own Time"
    , "Actual Loops"
    , "Plan Rows"
    , "Actual Rows"
    , "Plan Width"
    , "Workers Planned"
    , "Workers Launched"
    , "Parallel Aware"
    , "Group Key"
    , "Filter"
    , "Rows Removed by Filter"
    , "Sort Key"
    , "Sort Method"
    , "Sort Space Used"
    , "Sort Space Type"
    , "Join Type"
    , "Inner Unique"
    , "Join Filter"
    , "Hash Cond"
    , "Merge Cond"
    , "Rows Removed by Join Filter"
    , "Scan Direction"
    , "Index Name"
    , "Alias"
    , "Single Copy"
    , "Hash Buckets"
    , "Original Hash Buckets"
    , "Hash Batches"
    , "Original Hash Batches"
    , "Peak Memory Usage"
    , "Parallel Workers"
    , "Gather Node Depth"
  FROM local_query_node_stats;
END;
$BODY$ LANGUAGE plpgsql PARALLEL SAFE;

-- SELECT analyze_query($$
--   select
--     100.00 * sum(case
--         when p_type like 'PROMO%'
--             then l_extendedprice * (1 - l_discount)
--         else 0
--     end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
--   from
--     lineitem,
--     part
--   where
--     l_partkey = p_partkey
--     and l_shipdate >= date '1996-06-01'
--     and l_shipdate < date '1996-06-01' + interval '1' month
-- $$);

-- -- SELECT "Node Type", "Custom Plan Provider", "Partial Mode", "Parallel Aware", "Actual Loops", "Workers Planned", "Workers Launched", "Own Cost", "Own Time", "Actual Startup Time"
-- SELECT *
-- FROM query_node_stats
-- ORDER BY "Own Time" DESC;
