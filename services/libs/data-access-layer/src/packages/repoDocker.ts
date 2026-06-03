import { QueryExecutor } from '../queryExecutor'

// Database access layer for the dockerhub-sync worker. All repo_docker /
// repo_docker_pulls_daily / repos.docker_checked_at queries used by
// services/apps/packages_worker/src/dockerhub live here; the worker keeps the
// HTTP clients, rate-limit handling, and loop orchestration.
//
// Tables are defined in backend/src/osspckgs/migrations/V1779710880__initial_schema.sql.

// ---- query result shapes ----

export interface StaleRepoDockerRow {
  id: string
  repo_id: string | null
  image_name: string
}

export interface PendingDockerRepoRow {
  id: string
  url: string
  owner: string | null
  name: string | null
}

// ---- input shapes ----

export interface RepoDockerUpsertInput {
  repoId: string | null
  imageName: string
  pulls: number
  stars: number
}

// ---- reads ----

// repo_docker rows whose last_synced_at is older than the refresh interval,
// keyset-paginated by id. Backed by repo_docker_stale_idx.
export async function fetchStaleRepoDocker(
  qx: QueryExecutor,
  cursor: string | null,
  batchSize: number,
  refreshIntervalHours: number,
): Promise<StaleRepoDockerRow[]> {
  return qx.select(
    `
    SELECT id, repo_id, image_name
    FROM repo_docker
    WHERE last_synced_at < NOW() - make_interval(hours => $(refreshIntervalHours))
      AND ($(cursor)::bigint IS NULL OR id > $(cursor)::bigint)
    ORDER BY id
    LIMIT $(batchSize)
    `,
    { cursor, batchSize, refreshIntervalHours },
  )
}

// GitHub repos that have never been probed for a Docker image (or whose probe
// is older than discoveryIntervalDays), keyset-paginated by id. The
// docker_checked_at IS NULL arm is backed by repos_docker_pending_idx.
export async function fetchPendingDockerRepos(
  qx: QueryExecutor,
  cursor: string | null,
  batchSize: number,
  discoveryIntervalDays: number,
): Promise<PendingDockerRepoRow[]> {
  return qx.select(
    `
    SELECT id, url, owner, name
    FROM repos
    WHERE host = 'github'
      AND (docker_checked_at IS NULL
           OR docker_checked_at < NOW() - make_interval(days => $(discoveryIntervalDays)))
      AND ($(cursor)::bigint IS NULL OR id > $(cursor)::bigint)
    ORDER BY id
    LIMIT $(batchSize)
    `,
    { cursor, batchSize, discoveryIntervalDays },
  )
}

// ---- writes ----

export async function upsertRepoDockerRow(
  qx: QueryExecutor,
  input: RepoDockerUpsertInput,
): Promise<void> {
  await qx.result(
    `
    INSERT INTO repo_docker (repo_id, image_name, pulls, stars, last_synced_at)
    VALUES ($(repoId), $(imageName), $(pulls), $(stars), NOW())
    ON CONFLICT (image_name) DO UPDATE SET
      repo_id        = COALESCE(repo_docker.repo_id, EXCLUDED.repo_id),
      pulls          = EXCLUDED.pulls,
      stars          = EXCLUDED.stars,
      last_synced_at = NOW()
    `,
    input,
  )
}

export async function upsertRepoDockerDailySnapshot(
  qx: QueryExecutor,
  imageName: string,
  pullsTotal: number,
): Promise<void> {
  await qx.result(
    `
    INSERT INTO repo_docker_pulls_daily (image_name, date, pulls_total)
    VALUES ($(imageName), CURRENT_DATE, $(pullsTotal))
    ON CONFLICT (image_name, date) DO UPDATE SET pulls_total = EXCLUDED.pulls_total
    `,
    { imageName, pullsTotal },
  )
}

// Bumps last_synced_at without changing pull/star data. Used when an image
// 404s on refresh so it drops out of the stale-queue without losing history.
export async function touchRepoDocker(qx: QueryExecutor, imageName: string): Promise<void> {
  await qx.result(`UPDATE repo_docker SET last_synced_at = NOW() WHERE image_name = $(imageName)`, {
    imageName,
  })
}

export async function markRepoDockerChecked(qx: QueryExecutor, repoId: string): Promise<void> {
  await qx.result(`UPDATE repos SET docker_checked_at = NOW() WHERE id = $(repoId)`, { repoId })
}
