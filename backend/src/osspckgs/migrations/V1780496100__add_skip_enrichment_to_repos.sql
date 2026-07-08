ALTER TABLE repos ADD COLUMN IF NOT EXISTS skip_enrichment boolean NOT NULL DEFAULT false;
