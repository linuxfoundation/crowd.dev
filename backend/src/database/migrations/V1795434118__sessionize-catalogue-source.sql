ALTER TABLE integration."sessionizeEventMappings"
    ADD COLUMN IF NOT EXISTS "catalogueSource" TEXT NULL,
    DROP CONSTRAINT "sessionizeEventMappings_holdReason_check",
    ADD CONSTRAINT "sessionizeEventMappings_holdReason_check"
        CHECK ("holdReason" IS NULL OR "holdReason" IN ('no_match', 'ambiguous', 'not_cvent', 'no_segment'));
