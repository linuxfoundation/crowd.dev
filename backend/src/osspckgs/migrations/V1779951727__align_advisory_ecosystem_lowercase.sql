-- Per ADR-0001 §OSV "Ecosystem normalization", ecosystem values stored in
-- packages-db are lowercase: 'npm' and 'maven' (never 'Maven'). The OSV worker
-- originally preserved OSV's titlecase 'Maven'; this migration backfills
-- existing rows. Safe because there are no rows with both 'Maven' and 'maven'
-- for the same (advisory_id, ecosystem, package_name) tuple — the worker has
-- only ever written one case.
UPDATE advisory_packages
SET ecosystem = LOWER(ecosystem)
WHERE ecosystem <> LOWER(ecosystem);

-- packages.ecosystem is populated by the npm/Maven registry workers, not by
-- osv-sync. It's normalized at write time in those workers, but the LOWER()
-- backfill is cheap and idempotent so we include it here for completeness.
UPDATE packages
SET ecosystem = LOWER(ecosystem)
WHERE ecosystem <> LOWER(ecosystem);
