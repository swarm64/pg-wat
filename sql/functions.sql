CREATE FUNCTION pgwat.explain_wrapper(sql_query TEXT, run_analyze BOOL) RETURNS TABLE(plan JSON) AS
$BODY$
BEGIN
  IF run_analyze THEN
    RETURN QUERY EXECUTE 'EXPLAIN (ANALYZE, FORMAT JSON) ' || sql_query;
  ELSE
    RETURN QUERY EXECUTE 'EXPLAIN (FORMAT JSON) ' || sql_query;
  END IF;
END;
$BODY$ LANGUAGE plpgsql PARALLEL SAFE SET search_path = pgwat, public;

CREATE TYPE pgwat.T_UNROLL_HELPER AS (
    gather_node_id INT
  , parallel_workers INT
);

CREATE FUNCTION pgwat.first_occurence_not_null(arr T_UNROLL_HELPER[]) RETURNS T_UNROLL_HELPER AS
$BODY$
DECLARE
  item RECORD;
BEGIN
  FOR item IN (
    SELECT
        t.idx -- this is the actual gather_node_id
      , t.parallel_workers
    FROM UNNEST(arr)
    WITH ORDINALITY AS t(_, parallel_workers, idx)
  ) LOOP
    IF item.parallel_workers IS NOT NULL THEN
      RETURN (item.idx, item.parallel_workers)::T_UNROLL_HELPER;
    END IF;
  END LOOP;

  RETURN (NULL::INT, NULL::INT)::T_UNROLL_HELPER;
END;
$BODY$ LANGUAGE plpgsql PARALLEL SAFE SET search_path = pgwat, public;

CREATE FUNCTION pgwat.parse_explain_plan(in_name VARCHAR, in_plan JSON) RETURNS TABLE(plan_id UUID) AS
$BODY$
DECLARE
  plan JSON;
BEGIN
  SELECT
    CASE
      WHEN json_typeof(in_plan) = 'array' THEN in_plan
      WHEN json_typeof(in_plan) = 'object' THEN json_build_array(in_plan)
    END
  INTO plan;

  DROP TABLE IF EXISTS local_query_node_stats;
  CREATE TEMP TABLE local_query_node_stats AS
  WITH RECURSIVE t(subplans, unroll_helper_arr) AS (
    SELECT
        subplans.* AS subplans
      , array_append('{}'::T_UNROLL_HELPER[], (
            NULL::INT
          , (SELECT
              COALESCE(
                  (plan->0->'Plan'->>'Workers Launched')::INT
                , (plan->0->'Plan'->>'Workers Planned')::INT
              )::INT
            )
          )::T_UNROLL_HELPER
        )
    FROM json_array_elements((SELECT plan->0->'Plan'->'Plans')) subplans
    UNION ALL
    SELECT
        subplans
      , unroll_helper_arr
    FROM (
      SELECT
          json_array_elements((SELECT subplans->'Plans')) AS subplans
        , array_append(unroll_helper_arr, (
              NULL::INT
            , COALESCE(
                  (subplans->>'Workers Launched')::INT
                , (subplans->>'Workers Planned')::INT
              )::INT
            )::T_UNROLL_HELPER
          ) AS unroll_helper_arr
      FROM t
    ) a
  )
  SELECT
      (rec).* AS subplans
    , COALESCE(
          (rec)."Workers Launched"
        , (rec)."Workers Planned"
        , (SELECT max(parallel_workers) FROM unnest(unroll_helper_arr) AS parallel_workers)
      ) AS "Parallel Workers"
    , (SELECT gather_node_id FROM first_occurence_not_null(unroll_helper_arr) LIMIT 1) AS "Gather Node Id"
  FROM (
    SELECT
        json_populate_record(null::exp_type, plan->0->'Plan') AS rec
      , '{NULL}'::T_UNROLL_HELPER[] AS unroll_helper_arr
    UNION ALL
    SELECT
        json_populate_record(null::exp_type, subplans) AS rec
      , unroll_helper_arr
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
        , GREATEST(0, ROUND(COALESCE(
                "Actual Total Time" *
                  CASE
                    WHEN ("Parallel Workers" IS NOT NULL) THEN 1
                    ELSE "Actual Loops"
                  END - x.child_time
              , "Actual Total Time"
            )::NUMERIC, 2)
          ) AS own_time
        , x.child_cost AS child_cost
        , "Custom Plan Provider" ILIKE '%s64%shuffle%' AS is_shuffle
      FROM local_query_node_stats
      LEFT JOIN LATERAL(
        SELECT
            COALESCE(SUM("Total Cost"), 0) AS child_cost
          , COALESCE(SUM("Actual Total Time" *
                CASE
                  WHEN ("Parallel Workers" IS NOT NULL) THEN 1
                  ELSE "Actual Loops"
                END
              ), 0
            ) AS child_time
        FROM json_populate_recordset(null::exp_type, local_query_node_stats."Plans")
      ) x ON true
    ) cost_calc
  ) UPDATE local_query_node_stats
  SET "Own Cost" = own.own_cost
    , "Own Time" = own.own_time
  FROM own
  WHERE local_query_node_stats.id = own.id;

  RETURN QUERY
  WITH new_stats AS(
    INSERT INTO query_node_stats
    SELECT
        nextval('query_node_stats_id_seq'::regclass)
      , in_name
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
      , "Index Used"
      , "Alias"
      , "Single Copy"
      , "Hash Buckets"
      , "Original Hash Buckets"
      , "Hash Batches"
      , "Original Hash Batches"
      , "Peak Memory Usage"
      , "Parallel Workers"
      , "Gather Node Id"
      , "Output"
    FROM local_query_node_stats
    RETURNING query_node_stats.plan_id AS plan_id
  ) SELECT new_stats.plan_id FROM new_stats GROUP BY 1;
END;
$BODY$ LANGUAGE plpgsql SET search_path = pgwat, public;

