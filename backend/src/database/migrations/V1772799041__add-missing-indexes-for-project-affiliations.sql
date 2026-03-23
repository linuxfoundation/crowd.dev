-- Add missing index on memberSegmentAffiliations for memberId lookups
create index concurrently if not exists "ix_memberSegmentAffiliations_memberId"
    on "memberSegmentAffiliations" ("memberId");

-- Add missing index on mv_maintainer_roles materialized view for memberId lookups
create index concurrently if not exists "ix_mv_maintainer_roles_memberId"
    on mv_maintainer_roles ("memberId");
