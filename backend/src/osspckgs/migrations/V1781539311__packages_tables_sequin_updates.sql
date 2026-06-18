-- ─── 1. package_dependencies (created_at, id) index ─────────────────────────
CREATE INDEX IF NOT EXISTS package_dependencies_created_at_id_idx
  ON package_dependencies (created_at, id);

-- ─── 2. repo_activity_snapshot replication ──────────────────────────────────
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_publication WHERE pubname = 'sequin_pub'
    ) AND NOT EXISTS (
        SELECT 1 FROM pg_publication_tables
        WHERE pubname = 'sequin_pub'
          AND schemaname = 'public'
          AND tablename = 'repo_activity_snapshot'
    ) THEN
        ALTER PUBLICATION sequin_pub ADD TABLE repo_activity_snapshot;
    END IF;
END$$;

ALTER TABLE public.repo_activity_snapshot REPLICA IDENTITY FULL;
