alter table "mergeActions" drop constraint if exists "mergeActions_actionBy_fkey";
alter table "mergeActions" alter column "actionBy" type text using "actionBy"::text;