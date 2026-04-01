-- Drop 4 unused activityRelations indexes (already dropped on prod 2026-03-27,
-- see ACTIVITYRELATIONS_INDEX_CLEANUP.md — IF EXISTS guards for idempotency)
alter table "activityRelations" drop constraint if exists "activityRelations_activityId_memberId_key";

drop index concurrently if exists "ix_activityRelations_memberId_segmentId_include";
drop index concurrently if exists "ix_activityRelations_organizationId_segmentId_include";
drop index concurrently if exists "ix_activityRelations_platform_username";

create index concurrently if not exists idx_osa_org_segment_membercount
    on "organizationSegmentsAgg" ("organizationId", "segmentId")
    include ("memberCount");