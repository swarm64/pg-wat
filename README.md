# PG WAT - PostgreSQL Workload Analyzer Tool

## Summary

This tool parses PostgreSQL EXPLAIN (ANALYZE) plans in raw SQL, extracts all
information and puts it without hierarchy into a table for further analysis.
The table can be used to calculate statistics around queries and their plans.


## Usage

1. Apply `sql/schema.sql` and `sql/functions.sql` to any PG DB of your choice.
2. Run `SELECT parse_explain_plan(<explain plan json>);`.
3. Query `query_node_stats` as much as you want.

