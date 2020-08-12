DROP SCHEMA IF EXISTS pgwat CASCADE;
CREATE SCHEMA pgwat;

SET search_path to pgwat,public;

\i sql/schema.sql
\i sql/functions.sql
\i sql/misc.sql
