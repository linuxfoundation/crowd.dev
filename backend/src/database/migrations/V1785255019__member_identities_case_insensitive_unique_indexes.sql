-- Enforce uniqueness on lower(value) instead of the original value.
-- `value` preserves the source's preferred casing; email values are stored in lowercase.
-- docs/adr/0015-how-cdp-stores-member-identities.md

create unique index concurrently if not exists "uix_memberIdentities_memberId_platform_type_lower_value"
    on "memberIdentities" ("memberId", platform, type, lower(value))
    where "deletedAt" is null;

create unique index concurrently if not exists "uix_memberIdentities_platform_type_lower_value_verified"
    on "memberIdentities" (platform, type, lower(value))
    where verified = true
      and "deletedAt" is null;

drop index concurrently if exists "uix_memberIdentities_memberId_platform_value_type";
drop index concurrently if exists "uix_memberIdentities_platform_value_type_verified";
