DROP FUNCTION IF EXISTS _calculate_cache_ratio(NUMERIC, NUMERIC);
CREATE FUNCTION _calculate_cache_ratio(sum_blks_read NUMERIC, sum_blks_hit NUMERIC)
RETURNS DOUBLE PRECISION AS $$
  SELECT COALESCE(
    ROUND(
      sum_blks_hit / NULLIF((sum_blks_read + sum_blks_hit), 0),
    2), 0.0
  )::DOUBLE PRECISION
$$ LANGUAGE SQL PARALLEL SAFE;

DROP FUNCTION IF EXISTS _within_range(TIMESTAMPTZ, TIMESTAMPTZ, INTERVAL, INTERVAL);
CREATE FUNCTION _within_range(t1 TIMESTAMPTZ, t2 TIMESTAMPTZ, lower INTERVAL, upper INTERVAL)
RETURNS BOOL AS $$
  SELECT CASE
    WHEN lower IS NOT NULL AND upper IS NOT NULL THEN (t2 - t1 >= lower) AND (t2 - t1) < upper
    WHEN lower IS NULL AND upper IS NOT NULL THEN (t2 - t1) < upper
    WHEN lower IS NOT NULL AND upper IS NULL THEN (t2 - t1) >= lower
  END
$$ LANGUAGE SQL PARALLEL SAFE;

DROP PROCEDURE IF EXISTS _s64da_impact_analyzer_print(INTERVAL);
CREATE PROCEDURE _s64da_impact_analyzer_print(duration INTERVAL) AS $$
DECLARE
  row RECORD;
BEGIN
  FOR row IN SELECT
      COUNT(*) AS num_queries
    , duration
    , SUM(CASE WHEN _within_range(query_start, query_stop, NULL, '60s'::INTERVAL) THEN 1 ELSE 0 END) AS fast
    , SUM(CASE WHEN _within_range(query_start, query_stop, '60s'::INTERVAL, '15min'::INTERVAL) THEN 1 ELSE 0 END) AS medium
    , SUM(CASE WHEN _within_range(query_start, query_stop, '15min'::INTERVAL, NULL) THEN 1 ELSE 0 END) AS slow
    , COALESCE(MAX(max_parallelism), 0) AS max_parallelism
    , COALESCE(MIN(max_parallelism), 0) AS min_parallelism
    , COALESCE(ROUND(AVG(max_parallelism)), 0) AS avg_parallelism
  FROM query_stats LOOP
    RAISE NOTICE 'Query: %', to_json(row);
  END LOOP;

  FOR row IN SELECT
    SUM(heap_blks_read) AS heap_disk
  , SUM(heap_blks_hit) AS heap_cache
  , _calculate_cache_ratio(SUM(heap_blks_read), SUM(heap_blks_hit)) AS heap_ratio
  , SUM(idx_blks_read) AS idx_disk
  , SUM(idx_blks_hit) AS idx_cache
  , _calculate_cache_ratio(SUM(idx_blks_read), SUM(idx_blks_hit)) AS idx_ratio
--  , SUM(toast_blks_read - toast_blks_read_base) AS toast_disk
--  , SUM(toast_blks_hit - toast_blks_hit_base) AS toast_cache
--  , _calculate_cache_ratio(SUM(toast_blks_read), SUM(toast_blks_hit)) AS toast_ratio
  , SUM(inserts) AS inserts
  , SUM(inserts_time) AS inserts_time
  , SUM(updates) AS updates
  , SUM(updates_time) AS updates_time
  , SUM(deletes) AS deletes
  , SUM(deletes_time) AS deletes_time
    FROM workload_stats LOOP
    RAISE NOTICE 'IO: %', to_json(row);
  END LOOP;
END;
$$ LANGUAGE plpgsql;

DROP PROCEDURE IF EXISTS s64da_impact_analyzer();
CREATE PROCEDURE s64da_impact_analyzer() AS $$
DECLARE
  own_datname VARCHAR;
  own_pid INT;
  ts_start TIMESTAMPTZ;
  duration INTERVAL;
BEGIN
  SET SESSION swarm64da.enable_auto_analyze = off;

  SELECT current_database() INTO own_datname;
  SELECT pg_backend_pid() INTO own_pid;
  SELECT NOW() INTO ts_start;

  RAISE NOTICE 'S64 DA Impact Analyzer on DB: %s', own_datname;

  DROP TABLE IF EXISTS query_stats;
  CREATE TEMP TABLE query_stats(
      pid INT
    , max_parallelism INT
    , query_start TIMESTAMPTZ
    , query_stop TIMESTAMPTZ
    , runtime_parallel INTERVAL
  );
  ALTER TABLE query_stats ADD PRIMARY KEY(pid, query_start);

  DROP TABLE IF EXISTS base_values;
  CREATE TEMP TABLE base_values AS
  SELECT
      relid
    , heap_blks_read
    , heap_blks_hit
    , idx_blks_read
    , idx_blks_hit
    , toast_blks_read
    , toast_blks_hit
    , n_tup_ins
    , n_tup_upd
    , n_tup_del
  FROM pg_statio_user_tables stat_io
  JOIN pg_stat_user_tables stat USING(relid)
  WHERE stat_io.schemaname NOT LIKE 'pg_temp%'
  ;

  DROP TABLE IF EXISTS workload_stats;
  CREATE TEMP TABLE workload_stats AS
  SELECT
      stat_io.relid
    , stat_io.schemaname
    , stat_io.relname
    , 0::BIGINT AS heap_blks_read
    , 0::BIGINT AS heap_blks_hit
    , 0::BIGINT AS idx_blks_read
    , 0::BIGINT AS idx_blks_hit
    , 0::BIGINT AS toast_blks_read
    , 0::BIGINT AS toast_blks_hit
    , 0::BIGINT AS inserts
    , 0::BIGINT AS inserts_time
    , 0::BIGINT AS updates
    , 0::BIGINT AS updates_time
    , 0::BIGINT AS deletes
    , 0::BIGINT AS deletes_time
  FROM pg_statio_user_tables stat_io
  JOIN pg_stat_user_tables stat USING(relid)
  WHERE stat_io.schemaname NOT LIKE 'pg_temp%';

  LOOP
    BEGIN
      DROP TABLE IF EXISTS current_activity;
      CREATE TEMP TABLE current_activity AS
      SELECT
          query_start
        , MIN(pid) AS parent_pid
        , COUNT(*) AS worker_count
      FROM pg_stat_activity
      WHERE datname = own_datname
        AND pid <> own_pid
        AND state = 'active'
      GROUP BY query_start;

      INSERT INTO query_stats(query_start, pid, max_parallelism, runtime_parallel)
      SELECT *, '0'::INTERVAL FROM current_activity
      ON CONFLICT DO NOTHING
      ;

      UPDATE query_stats qs
      SET runtime_parallel =
            CASE
              WHEN ca.worker_count > 1 THEN runtime_parallel + '1'::INTERVAL
              ELSE runtime_parallel
            END
        , query_stop = NOW()
      FROM current_activity ca
      WHERE qs.pid = ca.parent_pid AND qs.query_start = ca.query_start
      ;

      UPDATE workload_stats ws
      SET heap_blks_read = src.heap_blks_read - bv.heap_blks_read
        , heap_blks_hit = src.heap_blks_hit - bv.heap_blks_hit
        , idx_blks_read = src.idx_blks_read - bv.idx_blks_read
        , idx_blks_hit = src.idx_blks_hit - bv.idx_blks_hit
        , toast_blks_read = src.toast_blks_read - bv.toast_blks_read
        , toast_blks_hit = src.toast_blks_hit - bv.toast_blks_hit
      FROM pg_statio_user_tables src
      JOIN base_values bv USING(relid)
      WHERE ws.relid = src.relid;

      UPDATE workload_stats ws
      SET inserts = src.n_tup_ins - bv.n_tup_ins
        , updates = src.n_tup_upd - bv.n_tup_upd
        , deletes = src.n_tup_del - bv.n_tup_del
        , inserts_time = CASE WHEN (src.n_tup_ins - bv.n_tup_ins) > inserts THEN inserts_time + 1 ELSE inserts_time END
        , updates_time = CASE WHEN (src.n_tup_upd - bv.n_tup_upd) > updates THEN updates_time + 1 ELSE updates_time END
        , deletes_time = CASE WHEN (src.n_tup_del - bv.n_tup_del) > deletes THEN deletes_time + 1 ELSE deletes_time END
      FROM pg_stat_user_tables src
      JOIN base_values bv USING(relid)
      WHERE ws.relid = src.relid;

      SELECT NOW() - ts_start INTO duration;
      CALL _s64da_impact_analyzer_print(duration);
      PERFORM pg_sleep(1);

    EXCEPTION WHEN operator_intervention THEN
      EXIT;
    END;

    COMMIT;
  END LOOP;
END;
$$ LANGUAGE plpgsql;
