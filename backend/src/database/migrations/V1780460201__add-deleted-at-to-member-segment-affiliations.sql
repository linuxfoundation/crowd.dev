alter table "memberSegmentAffiliations"
    add column if not exists "deletedAt" timestamp with time zone default null;
