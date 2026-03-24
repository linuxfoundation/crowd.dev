--------------------------------------------------------------------------------
-- add deletedAt column for soft deletion
--------------------------------------------------------------------------------

alter table "memberIdentities"
    add column if not exists "deletedAt" timestamp with time zone;

--------------------------------------------------------------------------------
-- drop and reshape indexes that need tenantId removal or soft-delete support
--------------------------------------------------------------------------------

-- memberId lookup
drop index if exists "memberIdentities_memberId_index";

-- tenantId is being deprecated; drop tenant-scoped indexes so they can be
-- recreated without tenantId in the index key
drop index if exists "ix_memberIdentities_tenantId";
drop index if exists ix_memberidentities_tenantid_platform_value_type;
drop index if exists ix_memberidentities_tenantid_platform_lowervalue_type;

-- uniqueness indexes (will be recreated as partial)
drop index if exists "uix_memberIdentities_memberId_platform_value_type";
drop index if exists "uix_memberIdentities_platform_value_type_tenantId_verified";

-- search/performance indexes to be recreated as partial
drop index if exists "ix_memberIdentities_platform_type_lowervalue_memberId";
drop index if exists idx_member_identities_lower_value;
drop index if exists idx_memberidentities_email_verified_trgm;

--------------------------------------------------------------------------------
-- recreate unique indexes (partial)
--------------------------------------------------------------------------------

-- per-member uniqueness: active identities only
create unique index if not exists "uix_memberIdentities_memberId_platform_value_type"
    on "memberIdentities" ("memberId", platform, value, type)
    where "deletedAt" is null;

-- verified uniqueness: active + verified only
create unique index if not exists "uix_memberIdentities_platform_value_type_verified"
    on "memberIdentities" (platform, value, type)
    where verified = true
      and "deletedAt" is null;

--------------------------------------------------------------------------------
-- recreate performance indexes (partial)
--------------------------------------------------------------------------------

-- memberId lookups (hot path)
create index if not exists "idx_memberIdentities_memberId"
    on "memberIdentities" ("memberId")
    where "deletedAt" is null;

-- platform / type / value lookups (formerly tenant-scoped)
create index if not exists "idx_memberIdentities_platform_type_lower_value_memberId"
    on "memberIdentities" (platform, type, lower(value), "memberId")
    where "deletedAt" is null;

-- general value search
create index if not exists "idx_memberIdentities_lower_value"
    on "memberIdentities" (lower(value))
    where "deletedAt" is null;

-- email trigram search (verified only)
create index if not exists "idx_memberIdentities_email_verified_trgm"
    on "memberIdentities"
    using gin (lower(value) gin_trgm_ops)
    where verified = true
      and type = 'email'
      and "deletedAt" is null;
