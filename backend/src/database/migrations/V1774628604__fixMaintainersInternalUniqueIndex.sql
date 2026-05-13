-- Fix maintainersInternal unique index and clean up duplicates.
--
-- Problem:
--   The unique index on ("repoId", "identityId", "startDate", "endDate") fails to
--   prevent duplicates because startDate and endDate are NULL on insert, and
--   PostgreSQL treats NULL != NULL in unique indexes — so the constraint never fires.
--
-- Fix:
--   Unique index on ("repoId", "identityId", role). All three columns are NOT NULL,
--   so the uniqueness check always works. startDate/endDate become purely informational.

-- Step 1: Remove duplicate rows, keeping the most recently updated row per group.
DELETE FROM "maintainersInternal"
WHERE id IN (
    SELECT id FROM (
        SELECT
            id,
            ROW_NUMBER() OVER (
                PARTITION BY "repoId", "identityId", role
                ORDER BY "updatedAt" DESC NULLS LAST, "createdAt" DESC NULLS LAST
            ) AS rn
        FROM "maintainersInternal"
    ) ranked
    WHERE rn > 1
);

-- Step 2: Drop the existing unique index.
DROP INDEX IF EXISTS maintainers_internal_repo_identity_unique_idx;

-- Step 3: Create the correct unique index on non-nullable columns.
CREATE UNIQUE INDEX maintainers_internal_repo_identity_role_unique_idx
    ON "maintainersInternal" ("repoId", "identityId", role);
