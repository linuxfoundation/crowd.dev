-- The existing idx_memberIdentities_email_verified_trgm covers the right partial
-- conditions (verified = true, type = 'email', deletedAt IS NULL) but is a GIN
-- trigram index -- PostgreSQL will not use it for equality joins on
-- lower(mi.value) against input emails, for example:
--
--   JOIN ON lower(mi.value) = input_email
--   WHERE mi.verified = true AND mi.type = 'email' AND mi."deletedAt" IS NULL
--
-- The only usable B-tree index (idx_memberIdentities_lower_value) only carries
-- the partial condition deletedAt IS NULL, so PG must heap-fetch every matched
-- row to re-check verified and type, which degrades as the table grows.
--
-- This B-tree partial index lets the planner do a direct nested-loop index scan
-- (one lookup per input email) with no extra heap fetches for verified/type.

CREATE INDEX CONCURRENTLY IF NOT EXISTS "idx_memberIdentities_verified_email_lower_value"
    ON "memberIdentities" (lower(value))
    WHERE verified = true
      AND type = 'email'
      AND "deletedAt" IS NULL;
