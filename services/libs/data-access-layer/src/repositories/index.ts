import { Logger } from '@crowd/logging'
import { RedisCache, RedisClient } from '@crowd/redis'

import { QueryExecutor } from '../queryExecutor'

/**
 * Repository entity from the public.repositories table
 */
export interface IRepository {
  id: string
  url: string
  segmentId: string
  gitIntegrationId: string
  sourceIntegrationId: string
  insightsProjectId: string
  archived: boolean
  forkedFrom: string | null
  excluded: boolean
  enabled: boolean
  createdAt: string
  updatedAt: string
  deletedAt: string | null
  lastArchivedCheckAt: string | null
}

export interface ICreateRepository {
  id: string
  url: string
  segmentId: string
  gitIntegrationId: string
  sourceIntegrationId: string
  insightsProjectId: string
  archived?: boolean
  forkedFrom?: string | null
  excluded?: boolean
}

/**
 * Bulk inserts repositories into public.repositories and git.repositoryProcessing
 * @param qx - Query executor (should be transactional)
 * @param repositories - Array of repositories to insert
 */
export async function insertRepositories(
  qx: QueryExecutor,
  repositories: ICreateRepository[],
): Promise<void> {
  if (repositories.length === 0) {
    return
  }

  const values = repositories.map((repo) => ({
    id: repo.id,
    url: repo.url,
    segmentId: repo.segmentId,
    gitIntegrationId: repo.gitIntegrationId,
    sourceIntegrationId: repo.sourceIntegrationId,
    insightsProjectId: repo.insightsProjectId,
    archived: repo.archived ?? false,
    forkedFrom: repo.forkedFrom ?? null,
    excluded: repo.excluded ?? false,
  }))

  await qx.result(
    `
    INSERT INTO public.repositories (
      id,
      url,
      "segmentId",
      "gitIntegrationId",
      "sourceIntegrationId",
      "insightsProjectId",
      archived,
      "forkedFrom",
      excluded,
      "createdAt",
      "updatedAt"
    )
    SELECT
      v.id::uuid,
      v.url,
      v."segmentId"::uuid,
      v."gitIntegrationId"::uuid,
      v."sourceIntegrationId"::uuid,
      v."insightsProjectId"::uuid,
      v.archived::boolean,
      v."forkedFrom",
      v.excluded::boolean,
      NOW(),
      NOW()
    FROM jsonb_to_recordset($(values)::jsonb) AS v(
      id text,
      url text,
      "segmentId" text,
      "gitIntegrationId" text,
      "sourceIntegrationId" text,
      "insightsProjectId" text,
      archived boolean,
      "forkedFrom" text,
      excluded boolean
    )
    `,
    { values: JSON.stringify(values) },
  )

  // Insert into git.repositoryProcessing to sync into git integration worker
  const repositoryIds = repositories.map((repo) => ({ repositoryId: repo.id }))
  await qx.result(
    `
    INSERT INTO git."repositoryProcessing" (
      "repositoryId",
      "createdAt",
      "updatedAt"
    )
    SELECT
      v."repositoryId"::uuid,
      NOW(),
      NOW()
    FROM jsonb_to_recordset($(repositoryIds)::jsonb) AS v(
      "repositoryId" text
    )
    `,
    { repositoryIds: JSON.stringify(repositoryIds) },
  )
}

/**
 * Get repositories by source integration ID
 * @param qx - Query executor
 * @param sourceIntegrationId - The source integration ID
 * @returns Array of repositories for the given integration (excluding soft-deleted)
 */
export async function getRepositoriesBySourceIntegrationId(
  qx: QueryExecutor,
  sourceIntegrationId: string,
): Promise<IRepository[]> {
  return qx.select(
    `
    SELECT
      id,
      url,
      "segmentId",
      "gitIntegrationId",
      "sourceIntegrationId",
      "insightsProjectId",
      archived,
      "forkedFrom",
      excluded,
      "createdAt",
      "updatedAt",
      "deletedAt",
      "lastArchivedCheckAt"
    FROM public.repositories
    WHERE "sourceIntegrationId" = $(sourceIntegrationId)
      AND "deletedAt" IS NULL
    `,
    { sourceIntegrationId },
  )
}

/**
 * Get repositories by their URLs
 * @param qx - Query executor
 * @param repoUrls - Array of repository URLs to search for
 * @param includeSoftDeleted - Whether to include soft-deleted repositories (default: false)
 * @returns Array of repositories matching the given URLs
 */
export async function getRepositoriesByUrl(
  qx: QueryExecutor,
  repoUrls: string[],
  includeSoftDeleted = false,
): Promise<IRepository[]> {
  if (repoUrls.length === 0) {
    return []
  }

  const deletedFilter = includeSoftDeleted ? '' : 'AND "deletedAt" IS NULL'

  return qx.select(
    `
    SELECT
      id,
      url,
      "segmentId",
      "gitIntegrationId",
      "sourceIntegrationId",
      "insightsProjectId",
      archived,
      "forkedFrom",
      excluded,
      "createdAt",
      "updatedAt",
      "deletedAt",
      "lastArchivedCheckAt"
    FROM public.repositories
    WHERE url IN ($(repoUrls:csv))
    ${deletedFilter}
    `,
    { repoUrls },
  )
}

/**
 * Soft deletes repositories by setting deletedAt = NOW()
 * Only deletes repos matching both the URLs and sourceIntegrationId
 * @param qx - Query executor
 * @param urls - Array of repository URLs to soft delete
 * @param sourceIntegrationId - Only delete repos belonging to this integration
 * @returns Number of rows affected
 */
export async function softDeleteRepositories(
  qx: QueryExecutor,
  urls: string[],
  sourceIntegrationId: string,
): Promise<number> {
  if (urls.length === 0) {
    return 0
  }

  return qx.result(
    `
    UPDATE public.repositories
    SET "deletedAt" = NOW(), "updatedAt" = NOW()
    WHERE url IN ($(urls:csv))
      AND "sourceIntegrationId" = $(sourceIntegrationId)
      AND "deletedAt" IS NULL
    `,
    { urls, sourceIntegrationId },
  )
}

/**
 * Restores soft-deleted and/or updated repositories and resets their processing state
 * Updates fields in public.repositories and resets git.repositoryProcessing for re-onboarding
 * @param qx - Query executor
 * @param repositories - Array of repositories with url (required) and optional fields to update
 */
export async function restoreRepositories(
  qx: QueryExecutor,
  repositories: Partial<ICreateRepository>[],
): Promise<void> {
  if (repositories.length === 0) {
    return
  }

  const urls = repositories.map((repo) => repo.url).filter(Boolean) as string[]
  if (urls.length === 0) {
    return
  }

  const values = repositories.map((repo) => ({
    url: repo.url,
    segmentId: repo.segmentId ?? null,
    gitIntegrationId: repo.gitIntegrationId ?? null,
    sourceIntegrationId: repo.sourceIntegrationId ?? null,
    insightsProjectId: repo.insightsProjectId ?? null,
    archived: repo.archived ?? null,
    forkedFrom: repo.forkedFrom ?? null,
    excluded: repo.excluded ?? null,
  }))

  await qx.result(
    `
    UPDATE public.repositories r
    SET
      "segmentId" = COALESCE(v."segmentId"::uuid, r."segmentId"),
      "gitIntegrationId" = COALESCE(v."gitIntegrationId"::uuid, r."gitIntegrationId"),
      "sourceIntegrationId" = COALESCE(v."sourceIntegrationId"::uuid, r."sourceIntegrationId"),
      "insightsProjectId" = COALESCE(v."insightsProjectId"::uuid, r."insightsProjectId"),
      archived = COALESCE(v.archived::boolean, r.archived),
      "forkedFrom" = COALESCE(v."forkedFrom", r."forkedFrom"),
      excluded = COALESCE(v.excluded::boolean, r.excluded),
      enabled = true,
      "deletedAt" = NULL,
      "updatedAt" = NOW()
    FROM jsonb_to_recordset($(values)::jsonb) AS v(
      url text,
      "segmentId" text,
      "gitIntegrationId" text,
      "sourceIntegrationId" text,
      "insightsProjectId" text,
      archived boolean,
      "forkedFrom" text,
      excluded boolean
    )
    WHERE r.url = v.url
    `,
    { values: JSON.stringify(values) },
  )

  // Reset git.repositoryProcessing for git re-onboarding
  await qx.result(
    `
    UPDATE git."repositoryProcessing" rp
    SET
      "lastProcessedAt" = NULL,
      "lastProcessedCommit" = NULL,
      "lastMaintainerRunAt" = NULL,
      branch = NULL,
      "lockedAt" = NULL,
      state = 'pending',
      "updatedAt" = NOW()
    FROM public.repositories r
    WHERE rp."repositoryId" = r.id
      AND r.url IN ($(urls:csv))
    `,
    { urls },
  )
}

/**
 * Repository mapping result with segment info
 */
export interface IRepositoryMapping {
  url: string
  segment: {
    id: string
    name: string
  }
  gitIntegrationId: string
  sourceIntegrationId: string
  sourcePlatform: string
}

/**
 * Get repository mappings for a specific integration
 * Replaces get{Github/Gitlab}Repos with a unified approach for all code platforms
 * Matches repos where gitIntegrationId OR sourceIntegrationId equals the given integrationId
 * @param qx - Query executor
 * @param integrationId - The integration ID (git or source platform) to filter by
 * @returns Array of repositories with segment info and integration IDs
 */
export async function getIntegrationReposMapping(
  qx: QueryExecutor,
  integrationId: string,
): Promise<IRepositoryMapping[]> {
  return qx.select(
    `
    SELECT
      r.url,
      jsonb_build_object(
        'id', s.id,
        'name', s.name
      ) as segment,
      r."gitIntegrationId",
      r."sourceIntegrationId",
      i.platform as "sourcePlatform"
    FROM public.repositories r
    JOIN segments s ON s.id = r."segmentId"
    LEFT JOIN integrations i ON i.id = r."sourceIntegrationId"
    WHERE (r."gitIntegrationId" = $(integrationId) OR r."sourceIntegrationId" = $(integrationId))
      AND r."deletedAt" IS NULL
    ORDER BY r.url
    `,
    { integrationId },
  )
}

/**
 * Upserts a single repository into public.repositories and git.repositoryProcessing
 * - If new: inserts into both tables
 * - If soft-deleted: restores and resets processing state
 * - If exists and not deleted: no-op (does not update)
 *
 * @param qx - Query executor
 * @param repository - Repository data to upsert
 * @returns 'inserted' | 'restored' | 'exists' indicating what action was taken
 */
export async function upsertRepository(
  qx: QueryExecutor,
  repository: ICreateRepository,
): Promise<'inserted' | 'restored' | 'exists'> {
  const existing = await qx.selectOneOrNone(
    `
    SELECT id, "deletedAt"
    FROM public.repositories
    WHERE url = $(url)
    `,
    { url: repository.url },
  )

  if (existing) {
    if (existing.deletedAt) {
      await restoreRepositories(qx, [repository])
      return 'restored'
    }
    // Already exists and not deleted - no-op
    return 'exists'
  }

  // Insert new repository
  await insertRepositories(qx, [repository])
  return 'inserted'
}

export interface IRepoSegmentLookup {
  integrationId: string
  url: string
  segmentId: string | undefined
}

let repoSegmentLookupCache: RedisCache | undefined

const CACHE_TTL_SECONDS = 7 * 24 * 60 * 60 // 7 days

function getRepoSegmentLookupCache(redis: RedisClient, log: Logger): RedisCache {
  if (!repoSegmentLookupCache) {
    repoSegmentLookupCache = new RedisCache('repoSegmentLookup', redis, log)
  }
  return repoSegmentLookupCache
}

/**
 * Find segment IDs for repositories by sourceIntegrationId and URL (no caching)
 * @param qx - Query executor
 * @param toFind - Array of { integrationId, url } to look up (integrationId = sourceIntegrationId)
 * @returns Array of { integrationId, url, segmentId } results
 */
async function findSegmentsForReposFromDb(
  qx: QueryExecutor,
  toFind: { integrationId: string; url: string }[],
): Promise<IRepoSegmentLookup[]> {
  if (toFind.length === 0) {
    return []
  }

  const orConditions: string[] = []
  const params: Record<string, string> = {}

  let index = 0
  for (const repo of toFind) {
    const urlKey = `url_${index}`
    const integrationKey = `integration_${index}`
    index++

    orConditions.push(`(url = $(${urlKey}) AND "sourceIntegrationId" = $(${integrationKey}))`)
    params[urlKey] = repo.url
    params[integrationKey] = repo.integrationId
  }

  const dbResults: { integrationId: string; url: string; segmentId: string }[] = await qx.select(
    `
    SELECT "sourceIntegrationId" AS "integrationId", url, "segmentId"
    FROM public.repositories
    WHERE "deletedAt" IS NULL AND (${orConditions.join(' OR ')})
    LIMIT ${toFind.length}
    `,
    params,
  )

  // Build results with undefined for not found repos
  return toFind.map((repo) => {
    const found = dbResults.find(
      (r) => r.integrationId === repo.integrationId && r.url === repo.url,
    )
    return {
      integrationId: repo.integrationId,
      url: repo.url,
      segmentId: found?.segmentId,
    }
  })
}

/**
 * Find segment ID for a single repository with caching
 * @param qx - Query executor
 * @param redis - Redis client for caching
 * @param log - Logger instance
 * @param integrationId - The source integration ID (GitHub/GitLab integration)
 * @param url - The repository URL
 * @returns The segment ID or null if not found
 */
export async function findSegmentForRepo(
  qx: QueryExecutor,
  redis: RedisClient,
  log: Logger,
  integrationId: string,
  url: string,
): Promise<string | null> {
  const results = await findSegmentsForRepos(qx, redis, log, [{ integrationId, url }])
  return results[0]?.segmentId ?? null
}

/**
 * Find segment IDs for multiple repositories with caching
 * First checks Redis cache, then queries DB for cache misses
 * Replaces the old GithubReposRepository/GitlabReposRepository pattern
 *
 * @param qx - Query executor
 * @param redis - Redis client for caching
 * @param log - Logger instance
 * @param toFind - Array of { integrationId, url } to look up
 * @returns Array of { integrationId, url, segmentId } results
 */
export async function findSegmentsForRepos(
  qx: QueryExecutor,
  redis: RedisClient,
  log: Logger,
  toFind: { integrationId: string; url: string }[],
): Promise<IRepoSegmentLookup[]> {
  if (toFind.length === 0) {
    return []
  }

  const cache = getRepoSegmentLookupCache(redis, log)
  const results: IRepoSegmentLookup[] = []

  const cachePromises: Promise<void>[] = []
  for (const repo of toFind) {
    cachePromises.push(
      (async () => {
        const key = `${repo.integrationId}:${repo.url}`
        const cached = await cache.get(key)
        if (cached) {
          results.push({
            integrationId: repo.integrationId,
            url: repo.url,
            segmentId: cached === 'null' ? undefined : cached,
          })
        }
      })().catch((err) => log.error(err, 'Error finding segment for repo in redis!')),
    )
  }

  await Promise.all(cachePromises)

  // Find repos that weren't in cache
  const remainingRepos = toFind.filter(
    (r) =>
      results.find((e) => e.integrationId === r.integrationId && e.url === r.url) === undefined,
  )

  if (remainingRepos.length > 0) {
    const dbResults = await findSegmentsForReposFromDb(qx, remainingRepos)

    const setCachePromises: Promise<void>[] = []
    for (const result of dbResults) {
      const key = `${result.integrationId}:${result.url}`
      results.push(result)

      if (result.segmentId) {
        setCachePromises.push(cache.set(key, result.segmentId, CACHE_TTL_SECONDS))
      } else {
        // Cache 'null' to prevent repeated DB lookups for non-existent repos
        setCachePromises.push(cache.set(key, 'null', CACHE_TTL_SECONDS))
      }
    }

    await Promise.all(setCachePromises)
  }

  const resolved = results.filter((r) => r.segmentId).length
  log.info(
    { total: toFind.length, cacheHits: toFind.length - remainingRepos.length, resolved },
    '[repoSegmentLookup] Lookup complete using public.repositories',
  )

  return results
}

export interface IGithubRepoForIntegration {
  url: string
  name: string
  owner: string
  forkedFrom: string | null
  updatedAt: string
}

export async function getReposForGithubIntegration(
  qx: QueryExecutor,
  integrationId: string,
): Promise<IGithubRepoForIntegration[]> {
  return qx.select(
    `
    SELECT
      r.url,
      split_part(r.url, '/', -1) as name,
      split_part(r.url, '/', -2) as owner,
      r."forkedFrom",
      r."updatedAt"
    FROM public.repositories r
    WHERE r."sourceIntegrationId" = $(integrationId)
      AND r."deletedAt" IS NULL
    ORDER BY r.url
    `,
    { integrationId },
  )
}

interface IGithubRepoForIntegrationWithSource extends IGithubRepoForIntegration {
  sourceIntegrationId: string
}

export async function getReposGroupedByOrgForIntegrations(
  qx: QueryExecutor,
  integrationIds: string[],
): Promise<Record<string, Record<string, IGithubRepoForIntegration[]>>> {
  if (integrationIds.length === 0) return {}

  const repos: IGithubRepoForIntegrationWithSource[] = await qx.select(
    `
    SELECT
      r.url,
      split_part(r.url, '/', -1) as name,
      split_part(r.url, '/', -2) as owner,
      r."forkedFrom",
      r."updatedAt",
      r."sourceIntegrationId"
    FROM public.repositories r
    WHERE r."sourceIntegrationId" IN ($(integrationIds:csv))
      AND r."deletedAt" IS NULL
    ORDER BY r.url
    `,
    { integrationIds },
  )

  const result: Record<string, Record<string, IGithubRepoForIntegration[]>> = {}
  for (const repo of repos) {
    const intId = repo.sourceIntegrationId
    if (!result[intId]) {
      result[intId] = {}
    }
    if (!result[intId][repo.owner]) {
      result[intId][repo.owner] = []
    }
    result[intId][repo.owner].push({
      url: repo.url,
      name: repo.name,
      owner: repo.owner,
      forkedFrom: repo.forkedFrom,
      updatedAt: repo.updatedAt,
    })
  }
  return result
}

/**
 * Populates `settings.orgs[].repos` from the repositories table for github/github-nango integrations.
 * Used by worker services to inject repo data into settings before passing to stream processors.
 */
export async function populateGithubSettingsWithRepos(
  qx: QueryExecutor,
  integrationId: string,
  settings: unknown,
): Promise<unknown> {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const s = settings as any
  if (!s?.orgs || !Array.isArray(s.orgs)) return settings

  const allReposByOrg = await getReposGroupedByOrgForIntegrations(qx, [integrationId])
  const reposByOrg = allReposByOrg[integrationId] || {}

  // Only overwrite orgs[].repos if the repositories table has data.
  // During the 'mapping' phase (legacy github connect), repos live in settings
  // before being written to the repositories table via mapGithubRepos.
  const hasRepos = Object.keys(reposByOrg).length > 0
  if (!hasRepos) return settings

  return {
    ...s,
    orgs: s.orgs.map((org: { name: string; [key: string]: unknown }) => ({
      ...org,
      repos: (reposByOrg[org.name] || []).map((r) => ({
        url: r.url,
        name: r.name,
        owner: r.owner,
        updatedAt: r.updatedAt,
        forkedFrom: r.forkedFrom,
      })),
    })),
  }
}

/**
 * Strips repos, unavailableRepos, and orgs[].repos from integration settings in the DB.
 * Called after mapUnifiedRepositories has written repos to the repositories table,
 * so they are no longer duplicated in both places.
 */
export async function stripReposFromGithubSettings(
  qx: QueryExecutor,
  integrationId: string,
): Promise<void> {
  await qx.result(
    `
    UPDATE integrations
    SET settings = jsonb_set(
      settings - 'repos' - 'unavailableRepos',
      '{orgs}',
      COALESCE(
        (SELECT jsonb_agg(org - 'repos') FROM jsonb_array_elements(settings->'orgs') org),
        '[]'::jsonb
      )
    )
    WHERE id = $(integrationId)
      AND settings->'orgs' IS NOT NULL
    `,
    { integrationId },
  )
}

/**
 * Updates repositories.enabled based on the provided list of enabled URLs.
 * Called when user toggles repository enabled status in the UI.
 */
export async function syncRepositoriesEnabledStatus(
  qx: QueryExecutor,
  insightsProjectId: string,
  enabledUrls: string[],
): Promise<void> {
  const normalizedUrls = enabledUrls.map((url) => url.toLowerCase())

  await qx.result(
    `
    UPDATE public.repositories
    SET 
      enabled = LOWER(url) = ANY($(normalizedUrls)::text[]),
      "updatedAt" = NOW()
    WHERE "insightsProjectId" = $(insightsProjectId)
      AND "deletedAt" IS NULL
      AND enabled <> (LOWER(url) = ANY($(normalizedUrls)::text[]))
    `,
    { insightsProjectId, normalizedUrls },
  )
}
