-- Add the showAggregateTabs column to the collections table, defaulting to false.
ALTER TABLE collections
    ADD COLUMN "showAggregateTabs" boolean DEFAULT false NOT NULL;

-- Only LF Foundation (curated) collections get the in-depth aggregate tabs by default.
-- ssoUserId is only set for community/user-curated collections (see V1772438175), so
-- ssoUserId IS NULL identifies curated collections.
UPDATE collections
SET "showAggregateTabs" = true
WHERE "ssoUserId" IS NULL;
