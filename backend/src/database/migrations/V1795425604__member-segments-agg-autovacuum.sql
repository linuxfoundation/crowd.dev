-- At the default scale factor autovacuum waits for ~49M dead rows on 243M. The aggregate backfill
-- rewrites ~49M rows a night and the daily sinks keep updating, so vacuum at 5M dead rows instead,
-- as on "activityRelations" (one index pass with the 64 MB maintenance_work_mem).
alter table "memberSegmentsAgg" set (
  autovacuum_vacuum_scale_factor = 0,
  autovacuum_vacuum_threshold = 5000000,
  autovacuum_vacuum_cost_limit = 1000,
  autovacuum_analyze_scale_factor = 0,
  autovacuum_analyze_threshold = 5000000
);
