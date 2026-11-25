-- The table has no unique key, so skip rows that already exist.
INSERT INTO "activityTypes" ("activityType", platform, "isCodeContribution", "isCollaboration", description, "label")
SELECT v."activityType", v.platform, false, false, v.description, v."label"
FROM (VALUES
    ('submitted-session', 'sessionize', 'Submitted a session to a call for papers', 'Submitted session'),
    ('spoke-at-session', 'sessionize', 'Spoke at an accepted session', 'Spoke at session')
) AS v("activityType", platform, description, "label")
WHERE NOT EXISTS (
    SELECT 1
    FROM "activityTypes" a
    WHERE a."activityType" = v."activityType"
      AND a.platform = v.platform
);
