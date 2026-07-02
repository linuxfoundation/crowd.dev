ALTER TABLE package_maintainers
    ADD COLUMN IF NOT EXISTS ingestion_source text;
