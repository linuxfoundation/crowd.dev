-- The table has no unique key, so skip rows that already exist (e.g. mention/twitter).
INSERT INTO "activityTypes" ("activityType", platform, "isCodeContribution", "isCollaboration", description, "label")
SELECT v."activityType", v.platform, false, false, v.description, v."label"
FROM (VALUES
    ('mention', 'github', 'Mentioned the project on GitHub', 'Mentioned the project'),
    ('mention', 'twitter', 'Mentioned the project on X/Twitter', 'Mentioned the project'),
    ('mention', 'linkedin', 'Mentioned the project on LinkedIn', 'Mentioned the project'),
    ('mention', 'reddit', 'Mentioned the project on Reddit', 'Mentioned the project'),
    ('mention', 'devto', 'Mentioned the project on DEV', 'Mentioned the project'),
    ('mention', 'hackernews', 'Mentioned the project on Hacker News', 'Mentioned the project'),
    ('mention', 'stackoverflow', 'Mentioned the project on Stack Overflow', 'Mentioned the project'),
    ('mention', 'medium', 'Mentioned the project on Medium', 'Mentioned the project'),
    ('mention', 'producthunt', 'Mentioned the project on Product Hunt', 'Mentioned the project'),
    ('mention', 'facebook', 'Mentioned the project on Facebook', 'Mentioned the project'),
    ('mention', 'youtube', 'Mentioned the project on YouTube', 'Mentioned the project'),
    ('mention', 'bluesky', 'Mentioned the project on Bluesky', 'Mentioned the project'),
    ('mention', 'tiktok', 'Mentioned the project on TikTok', 'Mentioned the project'),
    ('mention', 'instagram', 'Mentioned the project on Instagram', 'Mentioned the project')
) AS v("activityType", platform, description, "label")
WHERE NOT EXISTS (
    SELECT 1
    FROM "activityTypes" a
    WHERE a."activityType" = v."activityType"
      AND a.platform = v.platform
);
