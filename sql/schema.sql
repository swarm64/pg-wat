CREATE TABLE query_plans(
    db VARCHAR(255)
  , ts TIMESTAMPTZ
  , query_name VARCHAR(255)
  , plan JSON
);

CREATE TABLE query_node_stats(
    id BIGSERIAL PRIMARY KEY
  , query_name VARCHAR(100)
  , plan_id UUID
  , node_id INTEGER
  , node_type VARCHAR(100)
  , custom_plan_provider VARCHAR(100)
  , partial_mode VARCHAR(100)
  , relation_name VARCHAR(100)
  , startup_cost DOUBLE PRECISION
  , total_cost DOUBLE PRECISION
  , own_cost DOUBLE PRECISION
  , actual_startup_time DOUBLE PRECISION
  , actual_total_time DOUBLE PRECISION
  , own_time DOUBLE PRECISION
  , actual_loops INTEGER
  , plan_rows BIGINT
  , actual_rows BIGINT
  , plan_width INTEGER
  , workers_planned INTEGER
  , workers_launched INTEGER
  , parallel_aware BOOL
  , group_key VARCHAR[]
  , filter VARCHAR
  , rows_removed_by_filter BIGINT
  , sort_key VARCHAR[]
  , sort_method VARCHAR
  , sort_space_used BIGINT
  , sort_space_type VARCHAR
  , join_type VARCHAR
  , inner_unique BOOL
  , join_filter VARCHAR
  , hash_cond VARCHAR
  , merge_cond VARCHAR
  , rows_removed_by_join_filter BIGINT
  , scan_direction VARCHAR
  , index_name VARCHAR
  , index_used VARCHAR
  , alias VARCHAR
  , single_copy BOOL
  , hash_buckets BIGINT
  , original_hash_buckets BIGINT
  , hash_batches BIGINT
  , original_hash_batches BIGINT
  , peak_memory_usage BIGINT
  , parallel_workers INT
  , gather_node_id INT
  , output VARCHAR[]
);

CREATE TYPE exp_type AS(
    "Node Type" VARCHAR(100)
  , "Custom Plan Provider" VARCHAR(100)
  , "Partial Mode" VARCHAR(100)
  , "Relation Name" VARCHAR(100)
  , "Startup Cost" DOUBLE PRECISION
  , "Total Cost" DOUBLE PRECISION
  , "Actual Startup Time" DOUBLE PRECISION
  , "Actual Total Time" DOUBLE PRECISION
  , "Actual Loops" INTEGER
  , "Plan Rows" BIGINT
  , "Actual Rows" BIGINT
  , "Plan Width" INTEGER
  , "Workers Planned" INTEGER
  , "Workers Launched" INTEGER
  , "Parallel Aware" BOOL
  , "Parent Relationship" VARCHAR
  , "Group Key" VARCHAR[]
  , "Filter" VARCHAR
  , "Rows Removed by Filter" BIGINT
  , "Sort Key" VARCHAR[]
  , "Sort Method" VARCHAR
  , "Sort Space Used" BIGINT
  , "Sort Space Type" VARCHAR
  , "Join Type" VARCHAR
  , "Inner Unique" BOOL
  , "Join Filter" VARCHAR
  , "Hash Cond" VARCHAR
  , "Merge Cond" VARCHAR
  , "Rows Removed by Join Filter" BIGINT
  , "Scan Direction" VARCHAR
  , "Index Name" VARCHAR
  , "Index Used" VARCHAR
  , "Alias" VARCHAR
  , "Single Copy" BOOL
  , "Hash Buckets" BIGINT
  , "Original Hash Buckets" BIGINT
  , "Hash Batches" BIGINT
  , "Original Hash Batches" BIGINT
  , "Peak Memory Usage" BIGINT
  , "Output" VARCHAR[]
  , "Plans" JSON
);
