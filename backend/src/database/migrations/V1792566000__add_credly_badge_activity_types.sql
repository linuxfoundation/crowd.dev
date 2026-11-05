-- The table has no unique key, so skip rows that already exist.
INSERT INTO "activityTypes" ("activityType", platform, "isCodeContribution", "isCollaboration", description, "label")
SELECT v."activityType", v.platform, false, false, v.description, v."label"
FROM (VALUES
    ('issued-badge', 'credly', 'Credly issued a badge to the earner', 'Issued badge'),
    ('accepted-badge', 'credly', 'The earner accepted the badge', 'Accepted badge'),
    ('rejected-badge', 'credly', 'The earner rejected the badge', 'Rejected badge'),
    ('revoked-badge', 'credly', 'The issuer revoked the badge', 'Revoked badge'),
    ('replaced-badge', 'credly', 'The badge was replaced by a newer one', 'Replaced badge'),
    ('expired-badge', 'credly', 'The badge passed its expiry date', 'Expired badge')
) AS v("activityType", platform, description, "label")
WHERE NOT EXISTS (
    SELECT 1
    FROM "activityTypes" a
    WHERE a."activityType" = v."activityType"
      AND a.platform = v.platform
);
