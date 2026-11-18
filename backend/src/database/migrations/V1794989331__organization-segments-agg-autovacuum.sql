-- The nightly aggregate upserts clear most pages' all-visible bit; a low dead-row threshold re-sets it
-- soon after them, keeping member-count sums for affiliation tie-breaks on heap-fetch-free index scans.
alter table "organizationSegmentsAgg" set (
  autovacuum_vacuum_scale_factor = 0,
  autovacuum_vacuum_threshold = 200000,
  autovacuum_vacuum_cost_limit = 1000
);
