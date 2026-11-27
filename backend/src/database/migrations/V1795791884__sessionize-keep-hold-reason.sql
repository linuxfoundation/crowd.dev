ALTER TABLE integration."sessionizeEventMappings"
    DROP CONSTRAINT "sessionizeEventMappings_held_reason_check",
    ADD CONSTRAINT "sessionizeEventMappings_held_reason_check"
        CHECK ("status" <> 'held' OR "holdReason" IS NOT NULL);
