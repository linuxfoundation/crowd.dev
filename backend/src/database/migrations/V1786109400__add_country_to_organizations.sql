-- organizations.country is a denormalized copy of the default country value 
-- from orgAttributes table for Insights, since Sequin only syncs organizations.
alter table "organizations"
  add column if not exists "country" text;
