-- Dev seed: one hardcoded mailing list (CM-1318), no onboarding UI yet.
-- Run against a local CDP postgres. Picks an arbitrary existing segment —
-- replace the subquery with a real segmentId for anything beyond local testing.

BEGIN;

WITH target_segment AS (
    SELECT id AS "segmentId" FROM segments ORDER BY "createdAt" LIMIT 1
),
new_integration AS (
    INSERT INTO public.integrations (
        id, platform, status, settings, "tenantId", "segmentId", "createdAt", "updatedAt"
    )
    SELECT
        uuid_generate_v4(),
        'mailinglist',
        'done',
        '{}'::jsonb,
        '875c38bd-2b1b-4e91-ad07-0cfbabb4c49f', -- DEFAULT_TENANT_ID
        target_segment."segmentId",
        NOW(),
        NOW()
    FROM target_segment
    RETURNING id AS "integrationId"
),
new_list AS (
    INSERT INTO mailinglist.lists (
        id, name, "sourceUrl", "segmentId", "integrationId", "createdAt", "updatedAt"
    )
    SELECT
        uuid_generate_v4(),
        'linux-serial',
        'https://lore.kernel.org/linux-serial',
        target_segment."segmentId",
        new_integration."integrationId",
        NOW(),
        NOW()
    FROM target_segment, new_integration
    RETURNING id AS "listId"
)
INSERT INTO mailinglist."listProcessing" (
    "listId", state, priority, "lastProcessedHeads", "createdAt", "updatedAt"
)
SELECT "listId", 'pending', 2, '{}'::jsonb, NOW(), NOW()
FROM new_list;

COMMIT;
