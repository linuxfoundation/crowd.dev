-- Query: findMembersByVerifiedUsernames
--
-- Joins memberIdentities on (platform, lower(value)) with WHERE:
--   verified = true AND type = 'username' AND deletedAt IS NULL
--
-- The existing idx_memberIdentities_platform_type_lower_value_memberId is
-- missing verified = true in its partial condition, so PostgreSQL must
-- heap-fetch every row to recheck verified, which is expensive when there
-- are many unverified identities.
--
-- This index adds verified = true to the partial condition and includes memberId
-- so the join to members can read memberId from the index without a heap fetch.
CREATE INDEX CONCURRENTLY IF NOT EXISTS "idx_memberIdentities_verified_username_platform_lower_value"
    ON "memberIdentities" (platform, lower(value), "memberId")
    WHERE verified = true
      AND type = 'username'
      AND "deletedAt" IS NULL;

-- Query: findMembersByVerifiedEmails
--
-- Joins memberIdentities on lower(value) with WHERE:
--   verified = true AND type = 'email' AND deletedAt IS NULL
--
-- The existing idx_memberIdentities_verified_email_lower_value has the right
-- partial condition but only stores lower(value) — no memberId. Every matched
-- index entry requires a heap fetch to get memberId for the join to members.
-- Under concurrent insert/update load, those heap fetches queue behind buffer
-- pin locks, causing multi-second delays even for small inputs.
--
-- This index adds memberId so the join to members can proceed without
-- touching the memberIdentities heap pages.
CREATE INDEX CONCURRENTLY IF NOT EXISTS "idx_memberIdentities_verified_email_lower_value_memberid"
    ON "memberIdentities" (lower(value), "memberId")
    WHERE verified = true
      AND type = 'email'
      AND "deletedAt" IS NULL;
