-- 1. Deduplicate existing memberSegmentAffiliations
DELETE FROM "memberSegmentAffiliations" a USING (
    SELECT MIN(id) as keep_id, "memberId", "segmentId", "organizationId", "dateStart", "dateEnd"
    FROM "memberSegmentAffiliations"
    GROUP BY "memberId", "segmentId", "organizationId", "dateStart", "dateEnd"
    HAVING COUNT(*) > 1
) b
WHERE a."memberId" = b."memberId"
AND a."segmentId" = b."segmentId"
AND a."organizationId" IS NOT DISTINCT FROM b."organizationId"
AND a."dateStart" IS NOT DISTINCT FROM b."dateStart"
AND a."dateEnd" IS NOT DISTINCT FROM b."dateEnd"
AND a.id <> b.keep_id;

-- 2. Add an index to prevent exact duplicates in the future
-- Using COALESCE ensures that NULL values are logically treated as equal 
-- across all supported PostgreSQL versions for the sake of uniqueness.
CREATE UNIQUE INDEX "uq_member_segment_affiliations" ON "memberSegmentAffiliations" (
    "memberId",
    "segmentId",
    COALESCE("organizationId", '00000000-0000-0000-0000-000000000000'::uuid),
    COALESCE("dateStart", '1970-01-01T00:00:00Z'::timestamp),
    COALESCE("dateEnd", '1970-01-01T00:00:00Z'::timestamp)
);
