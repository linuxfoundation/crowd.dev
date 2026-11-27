create table "organizationProjectMatches" (
    id                 uuid primary key default gen_random_uuid(),
    "organizationId"   uuid not null references organizations (id) on delete cascade,
    "segmentId"        uuid not null references segments (id) on delete cascade,
    status             text not null default 'pending'
                       check (status in ('pending', 'approved', 'rejected')),
    "detectedBy"       text not null
                       check ("detectedBy" in ('segment-created', 'organization-created', 'backfill')),
    "organizationName" text not null,
    "segmentName"      text not null,
    "decidedById"      uuid references users (id) on delete set null,
    "decidedAt"        timestamp with time zone,
    "createdAt"        timestamp with time zone not null default now(),
    "updatedAt"        timestamp with time zone not null default now(),
    unique ("organizationId", "segmentId")
);

create index "ix_organizationProjectMatches_pending"
    on "organizationProjectMatches" ("createdAt")
    where status = 'pending';

create index "ix_organizationProjectMatches_segmentId"
    on "organizationProjectMatches" ("segmentId");
