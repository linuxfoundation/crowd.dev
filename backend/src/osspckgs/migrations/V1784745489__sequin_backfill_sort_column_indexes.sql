-- Sequin backfills paginate with a keyset cursor of (sort column, primary key),
-- e.g. WHERE ("last_synced_at", "id", "package_id") >= ($1, $2, $3) ORDER BY ... LIMIT n.
-- Without these indexes every page is a full sort (28M+ rows on versions,
-- 44M+ on package_dependencies) and hits Sequin's per-query timeout.
-- CONCURRENTLY relies on flyway's -mixed=true to run outside a transaction.

CREATE INDEX CONCURRENTLY IF NOT EXISTS versions_last_synced_at_id_package_id_idx
  ON versions (last_synced_at, id, package_id);

CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_updated_at_id_depends_on_id_idx
  ON package_dependencies (updated_at, id, depends_on_id);
