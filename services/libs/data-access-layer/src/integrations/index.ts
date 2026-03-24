import { getServiceChildLogger } from '@crowd/logging'
import { IIntegration, PlatformType } from '@crowd/types'

import { QueryExecutor } from '../queryExecutor'
import { getReposForGithubIntegration } from '../repositories'
import { getReposBySegmentGroupedByPlatform } from '../segments'

const log = getServiceChildLogger('db.integrations')

export function normalizeRepoUrl(url: string): string {
  try {
    const parsed = new URL(url)

    // Normalize protocol to https
    parsed.protocol = 'https:'

    // Remove www. prefix and lowercase hostname
    parsed.hostname = parsed.hostname.replace(/^www\./, '').toLowerCase()

    // Lowercase path for GitHub/GitLab (case-insensitive platforms)
    if (parsed.hostname === 'github.com' || parsed.hostname === 'gitlab.com') {
      parsed.pathname = parsed.pathname.toLowerCase()
    }

    // Remove trailing slashes and .git suffix
    parsed.pathname = parsed.pathname.replace(/\/+$/, '').replace(/\.git$/, '')

    // Remove query string and hash
    parsed.search = ''
    parsed.hash = ''

    return parsed.toString()
  } catch {
    return url
  }
}

/**
 * Extracts a human-readable label from a repository URL.
 * For GitHub/GitLab: returns "owner/repo"
 * For others: returns the full URL
 */
export function extractLabelFromUrl(url: string): string {
  try {
    const parsed = new URL(url)
    if (
      parsed.hostname === 'github.com' ||
      parsed.hostname === 'gitlab.com' ||
      parsed.hostname.endsWith('.gitlab.com')
    ) {
      return parsed.pathname
        .slice(1)
        .replace(/\.git$/, '')
        .replace(/\/+$/, '')
    }
    return url
  } catch {
    return url
  }
}

/* eslint-disable @typescript-eslint/no-explicit-any */

/**
 * Fetches a list of global integrations based on the provided filters.
 *
 * @param {QueryExecutor} qx - The query executor object to perform database queries.
 * @param {string[]} status - An array of status values to filter the integrations.
 * @param {string | null} platform - The platform to filter integrations, or null to include all platforms.
 * @param {string} query - A search string to filter segment names.
 * @param {number} limit - The maximum number of results to return.
 * @param {number} offset - The number of results to skip before starting to collect the result set.
 * @return {Promise<IIntegration[]>} A promise that resolves to the list of integrations matching the filters.
 */
export async function fetchGlobalIntegrations(
  qx: QueryExecutor,
  status: string[],
  platform: string | null,
  query: string,
  limit: number,
  offset: number,
  segmentId?: string | null,
): Promise<IIntegration[]> {
  return qx.select(
    `
        SELECT i.id,
               i.platform,
               i.status,
               i.settings,
               i."segmentId",
               s.name,
               s."parentId",
               s."parentName",
               s."grandparentId",
               s."grandparentName"
        FROM "integrations" i
               JOIN segments s ON i."segmentId" = s.id
        WHERE i."status" = ANY ($(status)::text[])
          AND i."deletedAt" IS NULL
          AND ($(platform) IS NULL OR i."platform" = $(platform))
          AND ($(segmentId) IS NULL OR s.id = $(segmentId) OR s."parentId" = $(segmentId) OR s."grandparentId" = $(segmentId))
          AND s.name ILIKE $(query)
        LIMIT $(limit) OFFSET $(offset)
      `,
    {
      status,
      platform,
      segmentId,
      query: `%${query}%`,
      limit,
      offset,
    },
  )
}

/**
 * Fetches the count of global integrations based on the specified criteria.
 *
 * @param {QueryExecutor} qx - The query executor to run the database query.
 * @param {string[]} status - The array of statuses to filter integrations.
 * @param {string|null} platform - The platform to filter by, or null for all platforms.
 * @param {string} query - The query string to filter segment names.
 * @return {Promise<{ count: number }[]>} The promise that resolves to an array with the count of integrations.
 */
export async function fetchGlobalIntegrationsCount(
  qx: QueryExecutor,
  status: string[],
  platform: string | null,
  query: string,
  segmentId?: string | null,
): Promise<{ count: number }[]> {
  return qx.select(
    `
        SELECT COUNT(*)
        FROM "integrations" i
               JOIN segments s ON i."segmentId" = s.id
        WHERE i."status" = ANY ($(status)::text[])
          AND i."deletedAt" IS NULL
          AND ($(platform) IS NULL OR i."platform" = $(platform))
          AND ($(segmentId) IS NULL OR s.id = $(segmentId) OR s."parentId" = $(segmentId) OR s."grandparentId" = $(segmentId))
          AND s.name ILIKE $(query)
      `,
    {
      status,
      platform,
      segmentId,
      query: `%${query}%`,
    },
  )
}

/**
 * Fetches a list of global integrations that are not connected.
 *
 * @param {QueryExecutor} qx - The query executor to run the queries.
 * @param {string | null} platform - The specific platform to filter the integrations, or null for all platforms.
 * @param {string} query - The query string to filter by integration name.
 * @param {number} limit - The maximum number of integrations to return.
 * @param {number} offset - The number of integrations to skip before starting to collect the result set.
 *
 * @return {Promise<IIntegration[]>} A promise that resolves to an array of integrations not connected to the specified platform.
 */
export async function fetchGlobalNotConnectedIntegrations(
  qx: QueryExecutor,
  platform: string | null,
  query: string,
  limit: number,
  offset: number,
  segmentId?: string | null,
): Promise<IIntegration[]> {
  return qx.select(
    `
      WITH unique_platforms AS (SELECT DISTINCT platform
                                FROM public.integrations),
           connected_platforms AS (SELECT i.platform, s.id as "segmentId"
                                   FROM integrations i
                                          JOIN "segments" s ON i."segmentId" = s.id
                                   WHERE i."deletedAt" IS NULL)
      SELECT up.platform,
             s.id as "segmentId",
             s.name,
             s."parentId",
             s."parentName",
             s."grandparentId",
             s."grandparentName"
      FROM unique_platforms up
             JOIN segments s ON true
             LEFT JOIN connected_platforms cp
                       ON up.platform = cp.platform AND s.id = cp."segmentId"
      WHERE cp.platform IS NULL
        AND s."parentId" IS NOT NULL
        AND s."grandparentId" IS NOT NULL
        AND ($(platform) IS NULL OR up."platform" = $(platform))
        AND ($(segmentId) IS NULL OR s.id = $(segmentId) OR s."parentId" = $(segmentId) OR s."grandparentId" = $(segmentId))
        AND s.name ILIKE $(query)
      LIMIT $(limit) OFFSET $(offset)
    `,
    {
      platform,
      segmentId,
      query: `%${query}%`,
      limit,
      offset,
    },
  )
}

/**
 * Fetches the count of global integrations that are not connected.
 *
 * @param {QueryExecutor} qx - The query executor used to perform SQL queries.
 * @param {string|null} platform - The platform to filter results by (optional).
 * @param {string} query - The name pattern to filter segments by.
 * @return {Promise<{count: number}[]>} - A promise that resolves to an array of objects containing the count of not connected integrations.
 */
export async function fetchGlobalNotConnectedIntegrationsCount(
  qx: QueryExecutor,
  platform: string | null,
  query: string,
  segmentId?: string | null,
): Promise<{ count: number }[]> {
  return qx.select(
    `
      WITH unique_platforms AS (SELECT DISTINCT platform
                                FROM public.integrations),
           connected_platforms AS (SELECT i.platform, s.id as "segmentId"
                                   FROM integrations i
                                          JOIN "segments" s ON i."segmentId" = s.id
                                   WHERE i."deletedAt" IS NULL)
      SELECT COUNT(*)
      FROM unique_platforms up
             JOIN segments s ON true
             LEFT JOIN connected_platforms cp
                       ON up.platform = cp.platform AND s.id = cp."segmentId"
      WHERE cp.platform IS NULL
        AND s."parentId" IS NOT NULL
        AND s."grandparentId" IS NOT NULL
        AND ($(platform) IS NULL OR up."platform" = $(platform))
        AND ($(segmentId) IS NULL OR s.id = $(segmentId) OR s."parentId" = $(segmentId) OR s."grandparentId" = $(segmentId))
        AND s.name ILIKE $(query)
    `,
    {
      platform,
      segmentId,
      query: `%${query}%`,
    },
  )
}

/**
 * Fetches the count of integrations grouped by their status for a given optional platform.
 *
 * @param {QueryExecutor} qx - The query executor used to perform the database query.
 * @param {string | null} platform - The platform to filter the integrations by, or null if no platform filter is to be applied.
 * @return {Promise<{status: string, count: number}[]>} A promise that resolves to an array of objects, each containing a status and the corresponding count of integrations.
 */
export async function fetchGlobalIntegrationsStatusCount(
  qx: QueryExecutor,
  platform: string | null,
  segmentId?: string | null,
): Promise<{ status: string; count: number }[]> {
  return qx.select(
    `
        SELECT i.status,
               COUNT(*) AS count
        FROM "integrations" i
        WHERE i."deletedAt" IS NULL
          AND ($(platform) IS NULL OR i."platform" = $(platform))
          AND ($(segmentId) IS NULL OR i."segmentId" = $(segmentId) OR 
               EXISTS (SELECT 1 FROM segments s WHERE s.id = i."segmentId" AND (s."parentId" = $(segmentId) OR s."grandparentId" = $(segmentId))))
        GROUP BY i.status
    `,
    {
      platform,
      segmentId,
    },
  )
}

export interface INangoIntegrationData {
  id: string
  segmentId: string
  platform: string
  settings: any
  createdAt: string
}

export async function fetchIntegrationById(
  qx: QueryExecutor,
  id: string,
): Promise<INangoIntegrationData | null> {
  return qx.selectOneOrNone(
    `
      select id, platform, settings, "segmentId", "createdAt"
      from integrations
      where "deletedAt" is null and id = $(id)
    `,
    {
      id,
    },
  )
}

export async function fetchNangoIntegrationDataForCheck(
  qx: QueryExecutor,
  platforms: string[],
): Promise<INangoIntegrationData[]> {
  return qx.select(
    `
      select id, platform, settings, "segmentId", "createdAt"
      from integrations
      where platform in ($(platforms:csv)) and "deletedAt" is null
      order by "updatedAt" asc
    `,
    {
      platforms,
    },
  )
}

export async function fetchNangoIntegrationData(
  qx: QueryExecutor,
  platforms: string[],
): Promise<INangoIntegrationData[]> {
  return qx.select(
    `
      select id, platform, settings, "createdAt"
      from integrations
      where platform in ($(platforms:csv)) and "deletedAt" is null
      order by "updatedAt" asc
    `,
    {
      platforms,
    },
  )
}

export async function fetchNangoDeletedIntegrationData(
  qx: QueryExecutor,
  platforms: string[],
): Promise<INangoIntegrationData[]> {
  return qx.select(
    `
      select id, platform, settings, "createdAt"
      from integrations
      where platform in ($(platforms:csv)) and "deletedAt" is not null
      order by "updatedAt" asc
    `,
    {
      platforms,
    },
  )
}

export async function findIntegrationDataForNangoWebhookProcessing(
  qx: QueryExecutor,
  id: string,
): Promise<{
  id: string
  segmentId: string
  platform: string
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  settings: any
} | null> {
  return qx.selectOneOrNone(
    `
      select i.id,
             i.platform,
             i."segmentId",
             i.settings
      from integrations i
      where i."deletedAt" is null
        and (
          i.id = $(id)
          or (
            i.platform = $(platform)
            and exists (
              select 1 from integration.nango_mapping nm
              where nm."integrationId" = i.id and nm."connectionId" = $(id)
            )
          )
        )
    `,
    {
      id,
      platform: PlatformType.GITHUB_NANGO,
    },
  )
}

export interface INangoCursorRow {
  integrationId: string
  connectionId: string
  platform: string
  model: string
  cursor: string
  lastCheckedAt: string | null
  createdAt: string
  updatedAt: string
}

export async function setNangoCursor(
  qx: QueryExecutor,
  integrationId: string,
  connectionId: string,
  platform: string,
  model: string,
  cursor: string,
): Promise<void> {
  await qx.result(
    `
      INSERT INTO integration.nango_cursors ("integrationId", "connectionId", platform, model, cursor, "updatedAt")
      VALUES ($(integrationId), $(connectionId), $(platform), $(model), $(cursor), now())
      ON CONFLICT ("integrationId", "connectionId", model)
      DO UPDATE SET cursor = $(cursor), "updatedAt" = now()
    `,
    {
      integrationId,
      connectionId,
      platform,
      model,
      cursor,
    },
  )
}

export async function getNangoCursor(
  qx: QueryExecutor,
  integrationId: string,
  connectionId: string,
  model: string,
): Promise<string | null> {
  const row = await qx.selectOneOrNone(
    `
      SELECT cursor FROM integration.nango_cursors
      WHERE "integrationId" = $(integrationId) AND "connectionId" = $(connectionId) AND model = $(model)
    `,
    { integrationId, connectionId, model },
  )
  return row?.cursor ?? null
}

export async function clearNangoCursors(qx: QueryExecutor, integrationId: string): Promise<void> {
  await qx.result(
    `DELETE FROM integration.nango_cursors WHERE "integrationId" = $(integrationId)`,
    { integrationId },
  )
}

export async function clearNangoCursorForModel(
  qx: QueryExecutor,
  integrationId: string,
  connectionId: string,
  model: string,
): Promise<void> {
  await qx.result(
    `DELETE FROM integration.nango_cursors WHERE "integrationId" = $(integrationId) AND "connectionId" = $(connectionId) AND model = $(model)`,
    { integrationId, connectionId, model },
  )
}

export async function removeNangoCursorsByConnection(
  qx: QueryExecutor,
  integrationId: string,
  connectionId: string,
): Promise<void> {
  await qx.result(
    `DELETE FROM integration.nango_cursors WHERE "integrationId" = $(integrationId) AND "connectionId" = $(connectionId)`,
    { integrationId, connectionId },
  )
}

export async function updateNangoCursorLastCheckedAt(
  qx: QueryExecutor,
  integrationId: string,
  connectionId: string,
): Promise<void> {
  await qx.result(
    `UPDATE integration.nango_cursors SET "lastCheckedAt" = now() WHERE "integrationId" = $(integrationId) AND "connectionId" = $(connectionId)`,
    { integrationId, connectionId },
  )
}

export async function fetchNangoLastCheckedAt(
  qx: QueryExecutor,
  platforms: string[],
): Promise<{ integrationId: string; connectionId: string; lastCheckedAt: string | null }[]> {
  return qx.select(
    `
      SELECT nc."integrationId", nc."connectionId", MIN(nc."lastCheckedAt") as "lastCheckedAt"
      FROM integration.nango_cursors nc
      JOIN integrations i ON i.id = nc."integrationId"
      WHERE i.platform IN ($(platforms:csv))
        AND i."deletedAt" IS NULL
      GROUP BY nc."integrationId", nc."connectionId"
    `,
    { platforms },
  )
}

export async function fetchNangoCursorRowsForIntegration(
  qx: QueryExecutor,
  integrationId: string,
): Promise<INangoCursorRow[]> {
  return qx.select(
    `SELECT * FROM integration.nango_cursors WHERE "integrationId" = $(integrationId)`,
    { integrationId },
  )
}

export async function fetchIntegrationsForSegment(
  qx: QueryExecutor,
  segmentId: string,
): Promise<IIntegration[]> {
  return qx.select(
    `
      SELECT *
      FROM "integrations" i
      WHERE i."segmentId" = $(segmentId)
        AND i."deletedAt" IS NULL
    `,
    { segmentId },
  )
}

export async function removeGithubNangoConnection(
  qx: QueryExecutor,
  integrationId: string,
  connectionId: string,
): Promise<void> {
  await qx.result(
    `DELETE FROM integration.nango_mapping WHERE "integrationId" = $(integrationId) AND "connectionId" = $(connectionId)`,
    {
      integrationId,
      connectionId,
    },
  )
}

export async function addGithubNangoConnection(
  qx: QueryExecutor,
  integrationId: string,
  connectionId: string,
  owner: string,
  repoName: string,
): Promise<void> {
  await qx.result(
    `
    INSERT INTO integration.nango_mapping ("integrationId", "connectionId", owner, "repoName")
    VALUES ($(integrationId), $(connectionId), $(owner), $(repoName))
    ON CONFLICT ("integrationId", "connectionId")
    DO UPDATE SET owner = $(owner), "repoName" = $(repoName), "updatedAt" = now()
    `,
    {
      integrationId,
      connectionId,
      owner,
      repoName,
    },
  )
}

export async function addRepoToGitIntegration(
  qx: QueryExecutor,
  integrationId: string,
  repoUrl: string,
): Promise<void> {
  // Get the github integration to find its segmentId
  const githubIntegration = await qx.selectOneOrNone(
    `
    select "segmentId" from integrations where id = $(integrationId) and "deletedAt" is null
    `,
    { integrationId },
  )

  if (!githubIntegration) {
    log.warn({ integrationId }, 'GitHub integration not found!')
    return
  }

  // Find the git integration for this segment
  const gitIntegration = await qx.selectOneOrNone(
    `
    select id, settings from integrations 
    where "segmentId" = $(segmentId) 
    and platform = 'git' 
    and "deletedAt" is null
    `,
    { segmentId: githubIntegration.segmentId },
  )

  if (!gitIntegration) {
    log.warn({ segmentId: githubIntegration.segmentId }, 'Git integration not found for segment!')
    return
  }

  // Get existing remotes
  const existingRemotes = gitIntegration.settings?.remotes || []

  // Check if repo already exists
  if (existingRemotes.includes(repoUrl)) {
    log.debug({ repoUrl }, 'Repo already exists in git integration, skipping!')
    return
  }

  // Add new repo to remotes array
  const updatedRemotes = [...existingRemotes, repoUrl]

  // Update git integration settings
  await qx.result(
    `
    update integrations
    set settings = jsonb_set(settings, '{remotes}', $(remotes)::jsonb),
        "updatedAt" = now()
    where id = $(id)
    `,
    {
      id: gitIntegration.id,
      remotes: JSON.stringify(updatedRemotes),
    },
  )

  log.info({ integrationId: gitIntegration.id, repoUrl }, 'Added repo to git integration settings!')
}

export async function findNangoRepositoriesToBeRemoved(
  qx: QueryExecutor,
  integrationId: string,
): Promise<string[]> {
  const integration = await fetchIntegrationById(qx, integrationId)

  if (!integration || integration.platform !== PlatformType.GITHUB_NANGO) {
    return []
  }

  const allNangoMappings = await getNangoMappingsForIntegrations(qx, [integrationId])
  const nangoMappings = allNangoMappings[integrationId] || {}

  if (Object.keys(nangoMappings).length === 0) {
    return []
  }

  // Get desired repos from repositories table
  const repos = await getReposForGithubIntegration(qx, integrationId)
  const repoSlugs = new Set<string>(repos.map((r) => `${r.owner}/${r.name}`))

  // Find nango mappings that aren't in the desired set
  const reposToBeRemoved: string[] = []
  for (const mappedRepo of Object.values(nangoMappings)) {
    if (!repoSlugs.has(`${mappedRepo.owner}/${mappedRepo.repoName}`)) {
      reposToBeRemoved.push(`https://github.com/${mappedRepo.owner}/${mappedRepo.repoName}`)
    }
  }

  return reposToBeRemoved
}

export interface INangoMappingRow {
  integrationId: string
  connectionId: string
  repositoryId: string | null
  owner: string
  repoName: string
  createdAt: string
  updatedAt: string
}

export type NangoMappingEntry = { owner: string; repoName: string; repositoryId: string | null }

export async function getNangoMappingsForIntegrations(
  qx: QueryExecutor,
  integrationIds: string[],
): Promise<Record<string, Record<string, NangoMappingEntry>>> {
  if (integrationIds.length === 0) return {}

  const rows: INangoMappingRow[] = await qx.select(
    `SELECT * FROM integration.nango_mapping WHERE "integrationId" IN ($(integrationIds:csv))`,
    { integrationIds },
  )

  const result: Record<string, Record<string, NangoMappingEntry>> = {}
  for (const row of rows) {
    if (!result[row.integrationId]) {
      result[row.integrationId] = {}
    }
    result[row.integrationId][row.connectionId] = {
      owner: row.owner,
      repoName: row.repoName,
      repositoryId: row.repositoryId,
    }
  }
  return result
}

export async function getNangoMappingByConnectionId(
  qx: QueryExecutor,
  connectionId: string,
): Promise<INangoMappingRow | null> {
  return qx.selectOneOrNone(
    `SELECT * FROM integration.nango_mapping WHERE "connectionId" = $(connectionId)`,
    { connectionId },
  )
}

export async function linkNangoMappingToRepository(
  qx: QueryExecutor,
  integrationId: string,
  connectionId: string,
  repositoryId: string,
): Promise<void> {
  await qx.result(
    `
    UPDATE integration.nango_mapping
    SET "repositoryId" = $(repositoryId), "updatedAt" = now()
    WHERE "integrationId" = $(integrationId) AND "connectionId" = $(connectionId)
    `,
    { integrationId, connectionId, repositoryId },
  )
}

export async function findRepositoriesForSegment(
  qx: QueryExecutor,
  segmentId: string,
): Promise<Record<string, Array<{ url: string; label: string; enabled: boolean }>>> {
  // Get all repos grouped by platform (github-nango merged into github)
  const reposByPlatform = await getReposBySegmentGroupedByPlatform(qx, segmentId, true)

  // Transform to include URLs, labels, and enabled status
  const result: Record<string, Array<{ url: string; label: string; enabled: boolean }>> = {}

  for (const [platform, repos] of Object.entries(reposByPlatform)) {
    result[platform] = repos.map((repo) => ({
      url: repo.url,
      label: extractLabelFromUrl(repo.url),
      enabled: repo.enabled,
    }))
  }

  return result
}
