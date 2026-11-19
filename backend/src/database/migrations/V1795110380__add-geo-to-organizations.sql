-- organizations.geo is a denormalized copy of the default geo value
-- from orgAttributes table for Insights, since Sequin only syncs organizations.
alter table "organizations"
  add column if not exists "geo" jsonb;
