create table public."erasedMemberActivityFootprint" (
    "memberId" uuid not null,
    "segmentId" uuid not null,
    "segmentName" text,
    "parentName" text,
    "grandparentName" text,
    platform text not null,
    channel text not null default '',
    "activityCount" integer not null,
    "erasedAt" timestamp with time zone default now() not null,
    primary key ("memberId", "segmentId", platform, channel)
);
create index ix_erased_member_activity_footprint_segment on public."erasedMemberActivityFootprint" ("segmentId");

alter table public."requestedForErasureMemberIdentities" add column "memberId" uuid null;
create index ix_requested_for_erasure_memberidentities_member_id on public."requestedForErasureMemberIdentities" ("memberId");
