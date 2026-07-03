-- Sonatype popularity signal (first delivery: Maven P0, top 500 components).
-- Sonatype could not provide raw Maven download counts, so instead they deliver a
-- popularity score derived from their SCA telemetry: normalized 0-100 per ecosystem,
-- tiered P0-P3. Stored in dedicated columns (not JSONB) because score/rank/tier are
-- queried independently. Only sonatype_popularity_score feeds rank_packages(); rank and
-- tier are kept for display/audit. No index on first pass (dataset is small, 500-2k rows).
ALTER TABLE packages
    ADD COLUMN IF NOT EXISTS sonatype_popularity_score numeric,      -- 0-100, normalized per ecosystem; consumed by rank_packages()
    ADD COLUMN IF NOT EXISTS sonatype_rank              int,         -- rank within the Sonatype list; display/audit only
    ADD COLUMN IF NOT EXISTS sonatype_tier              text,        -- 'P0' | 'P1' | 'P2' | 'P3'
    ADD COLUMN IF NOT EXISTS sonatype_snapshot_at       timestamptz, -- timestamp of the Sonatype snapshot the row came from
    ADD COLUMN IF NOT EXISTS sonatype_updated_at        timestamptz; -- our last upsert of the sonatype_* fields
