-- At the default scale factor autovacuum waits for ~200M dead rows; 5M keeps each run to one
-- index pass with the 64 MB maintenance_work_mem and runs it well before an anti-wraparound vacuum.
alter table "activityRelations" set (
  autovacuum_vacuum_scale_factor = 0,
  autovacuum_vacuum_threshold = 5000000,
  autovacuum_vacuum_cost_limit = 1000,
  autovacuum_analyze_scale_factor = 0,
  autovacuum_analyze_threshold = 5000000
);
