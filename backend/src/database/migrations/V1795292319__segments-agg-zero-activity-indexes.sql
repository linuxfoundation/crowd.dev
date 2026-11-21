create index concurrently if not exists "ix_memberSegmentsAgg_zero_activity"
  on "memberSegmentsAgg" ("segmentId", "memberId")
  where "activityCount" = 0;
create index concurrently if not exists "ix_organizationSegmentsAgg_zero_activity"
  on "organizationSegmentsAgg" ("segmentId", "organizationId")
  where "activityCount" = 0;
