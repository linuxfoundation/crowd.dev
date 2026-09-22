-- dockerhub-sync (CM-1213)
--
-- Adds discovery/refresh bookkeeping for the dockerhub-sync worker
-- (services/apps/packages_worker/src/dockerhub) and a daily snapshot table
-- for Docker Hub lifetime pull counts.

-- Last time dockerhub-sync probed this repo for a published Docker image
-- (Dockerfile detection + Hub candidate lookup). NULL = never checked.
-- Separate from repos.last_synced_at because discovery cadence (weeks)
-- differs from light-metadata refresh cadence (daily).
ALTER TABLE repos
    ADD COLUMN IF NOT EXISTS docker_checked_at timestamptz;

-- Partial index for the discovery backlog query: pages repos that have never
-- been probed for a Docker image. Once docker_checked_at is set the row drops
-- out of the index, so this stays small even as the repos table grows.
CREATE INDEX IF NOT EXISTS repos_docker_pending_idx ON repos (id)
WHERE
    host = 'github' AND docker_checked_at IS NULL;

-- Supports the refresh query (WHERE last_synced_at < NOW() - interval).
CREATE INDEX IF NOT EXISTS repo_docker_stale_idx ON repo_docker (last_synced_at);

-- ============================================================
-- REPO DOCKER PULLS DAILY
-- One row per image per day storing the *lifetime* pull_count as returned
-- by hub.docker.com/v2/repositories/<image>. Docker Hub does not expose
-- per-day download counts, so daily deltas are derived at query time:
--   pulls_total - LAG(pulls_total) OVER (PARTITION BY image_name ORDER BY date)
-- Keyed by image_name (matches repo_docker UNIQUE) so rows survive a
-- repo_docker re-discovery without an FK cascade.
--
-- Partitioned monthly via pg_partman (extension + schema already created in
-- V1780231200__npm_worker.sql).
-- ============================================================
CREATE TABLE IF NOT EXISTS repo_docker_pulls_daily (
    image_name text NOT NULL,
    date date NOT NULL,
    pulls_total bigint NOT NULL,
    PRIMARY KEY (image_name, date)
)
PARTITION BY RANGE (date);

-- Guard so this migration is idempotent against environments where the
-- table was already registered manually (e.g. local dev that applied the
-- earlier in-place schema edit).
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM partman.part_config
    WHERE parent_table = 'public.repo_docker_pulls_daily'
  ) THEN
    PERFORM partman.create_parent(
      p_parent_table => 'public.repo_docker_pulls_daily',
      p_control      => 'date',
      p_interval     => '1 month',
      p_premake      => 3
    );
  END IF;
END
$$;
