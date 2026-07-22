CREATE INDEX CONCURRENTLY IF NOT EXISTS versions_last_synced_at_id_package_id_idx
  ON versions (last_synced_at, id, package_id);

CREATE INDEX CONCURRENTLY IF NOT EXISTS package_dependencies_updated_at_id_depends_on_id_idx
  ON package_dependencies (updated_at, id, depends_on_id);
