-- organizations.country is a denormalized default for Insights (Sequin syncs
-- organizations, not orgAttributes). Provenance still lives in orgAttributes
-- (source = system for inferred values), same dual-write pattern as location.
alter table "organizations"
  add column if not exists "country" text;
