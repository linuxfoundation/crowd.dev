-- The default 20% scale factor waits for ~49M dead rows on 243M; vacuum at 5M, as on "activityRelations".
alter table "memberSegmentsAgg" set (
  autovacuum_vacuum_scale_factor = 0,
  autovacuum_vacuum_threshold = 5000000,
  autovacuum_vacuum_cost_limit = 1000,
  autovacuum_analyze_scale_factor = 0,
  autovacuum_analyze_threshold = 5000000
);
