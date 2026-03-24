-- ---------------------------------------------------------------------------
-- Add verification metadata for member identities
-- ---------------------------------------------------------------------------

-- Track the original source of the identity and which source performed the latest verification
alter table "memberIdentities" add column if not exists "source" varchar(255);
alter table "memberIdentities" add column if not exists "verifiedBy" varchar(255);

-- ---------------------------------------------------------------------------
-- Add verification metadata for organization identities
-- ---------------------------------------------------------------------------

-- Track the original source of the identity
alter table "organizationIdentities" add column if not exists "source" varchar(255);

-- ---------------------------------------------------------------------------
-- Add verification metadata for work experiences
-- ---------------------------------------------------------------------------

-- Track if the work experience has been verified and by which source
alter table "memberOrganizations" add column if not exists "verified" boolean not null default false;
alter table "memberOrganizations" add column if not exists "verifiedBy" varchar(255);

-- ---------------------------------------------------------------------------
-- Align audit logging with a generic actor model
-- ---------------------------------------------------------------------------

-- Add actorId and actorType as nullable initially
alter table "auditLogAction" add column if not exists "actorId" varchar(255);
alter table "auditLogAction" add column if not exists "actorType" varchar(255);

-- Backfill historical rows (map old userId into new columns)
update "auditLogAction"
set "actorId" = "userId"::text,
    "actorType" = 'user'
where "actorId" is null;

-- Enforce NOT NULL after backfill
alter table "auditLogAction" alter column "actorId" set not null;
alter table "auditLogAction" alter column "actorType" set not null;

-- Add index on actorId to speed up queries
create index if not exists "auditLogAction_actorId" on "auditLogAction" ("actorId");

-- Remove old userId column
alter table "auditLogAction" drop column if exists "userId";

-- ---------------------------------------------------------------------------
-- Remove legacy auditLogs table
-- ---------------------------------------------------------------------------
-- This table is no longer used; all audit actions are tracked in auditLogAction
drop table if exists "auditLogs";