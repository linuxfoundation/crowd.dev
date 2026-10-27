import { getServiceChildLogger } from '@crowd/logging'
import {
  AUTO_MANAGED_INTEGRATION_PLATFORMS,
  IGithubIntegrationSettings,
  IIntegration,
  PlatformType,
} from '@crowd/types'

import { QueryExecutor } from '../queryExecutor'
import { getReposForGithubIntegration } from '../repositories'
import { getReposBySegmentGroupedByPlatform } from '../segments'

const log = getServiceChildLogger('db.integrations')

const ADMIN_NOT_CONNECTABLE_PLATFORMS: PlatformType[] = [
  ...AUTO_MANAGED_INTEGRATION_PLATFORMS,
  PlatformType.TWITTER,
  PlatformType.MAILINGLIST,
]

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
  isLF?: boolean | null,
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
          AND ($(isLF)::boolean IS NULL OR s."isLF" = $(isLF))
          AND s.name ILIKE $(query)
        ORDER BY s."grandparentName", s."parentName", s.name, i.platform, i.id
        LIMIT $(limit) OFFSET $(offset)
      `,
    {
      status,
      platform,
      segmentId,
      isLF: isLF ?? null,
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
  isLF?: boolean | null,
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
          AND ($(isLF)::boolean IS NULL OR s."isLF" = $(isLF))
          AND s.name ILIKE $(query)
      `,
    {
      status,
      platform,
      segmentId,
      isLF: isLF ?? null,
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
  isLF?: boolean | null,
  platforms?: string[] | null,
): Promise<IIntegration[]> {
  return qx.select(
    `
      WITH unique_platforms AS (SELECT DISTINCT platform
                                FROM (SELECT $(platform)::text AS platform
                                      UNION
                                      SELECT CASE WHEN platform = 'github-nango' THEN 'github' ELSE platform END
                                      FROM public.integrations) known
                                WHERE platform IS NOT NULL
                                  AND NOT platform = ANY ($(notConnectable)::text[])
                                  AND ($(platforms)::text[] IS NULL OR platform = ANY ($(platforms)::text[]))),
           connected_platforms AS (SELECT CASE WHEN i.platform = 'github-nango' THEN 'github' ELSE i.platform END AS platform,
                                          s.id as "segmentId"
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
        AND ($(isLF)::boolean IS NULL OR s."isLF" = $(isLF))
        AND s.name ILIKE $(query)
      ORDER BY s."grandparentName", s."parentName", s.name, up.platform, s.id
      LIMIT $(limit) OFFSET $(offset)
    `,
    {
      platform,
      segmentId,
      isLF: isLF ?? null,
      query: `%${query}%`,
      limit,
      offset,
      notConnectable: ADMIN_NOT_CONNECTABLE_PLATFORMS,
      platforms: platforms?.length ? platforms : null,
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
  isLF?: boolean | null,
  platforms?: string[] | null,
): Promise<{ count: number }[]> {
  return qx.select(
    `
      WITH unique_platforms AS (SELECT DISTINCT platform
                                FROM (SELECT $(platform)::text AS platform
                                      UNION
                                      SELECT CASE WHEN platform = 'github-nango' THEN 'github' ELSE platform END
                                      FROM public.integrations) known
                                WHERE platform IS NOT NULL
                                  AND NOT platform = ANY ($(notConnectable)::text[])
                                  AND ($(platforms)::text[] IS NULL OR platform = ANY ($(platforms)::text[]))),
           connected_platforms AS (SELECT CASE WHEN i.platform = 'github-nango' THEN 'github' ELSE i.platform END AS platform,
                                          s.id as "segmentId"
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
        AND ($(isLF)::boolean IS NULL OR s."isLF" = $(isLF))
        AND s.name ILIKE $(query)
    `,
    {
      platform,
      segmentId,
      isLF: isLF ?? null,
      query: `%${query}%`,
      notConnectable: ADMIN_NOT_CONNECTABLE_PLATFORMS,
      platforms: platforms?.length ? platforms : null,
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
  isLF?: boolean | null,
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
          AND ($(isLF)::boolean IS NULL OR
               EXISTS (SELECT 1 FROM segments s WHERE s.id = i."segmentId" AND s."isLF" = $(isLF)))
          AND NOT (COALESCE(i.platform = ANY ($(autoManaged)::text[]), false) AND i."segmentId" IS NULL)
        GROUP BY i.status
    `,
    {
      platform,
      segmentId,
      isLF: isLF ?? null,
      autoManaged: AUTO_MANAGED_INTEGRATION_PLATFORMS,
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

export async function findIntegrationPlatformsByIds(
  qx: QueryExecutor,
  ids: string[],
): Promise<string[]> {
  if (ids.length === 0) {
    return []
  }

  const rows: { platform: string }[] = await qx.select(
    `SELECT DISTINCT platform FROM integrations WHERE id = ANY ($(ids)::uuid[])`,
    { ids },
  )
  return rows.map((r) => r.platform)
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
  model: string,
): Promise<void> {
  await qx.result(
    `UPDATE integration.nango_cursors
     SET "lastCheckedAt" = now()
     WHERE "integrationId" = $(integrationId) AND "connectionId" = $(connectionId) AND model = $(model)`,
    { integrationId, connectionId, model },
  )
}

export async function getNangoModelLastCheckedAt(
  qx: QueryExecutor,
  integrationId: string,
  connectionId: string,
  model: string,
): Promise<string | null> {
  const row = await qx.selectOneOrNone(
    `SELECT to_char("lastCheckedAt" AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS "lastCheckedAt"
     FROM integration.nango_cursors
     WHERE "integrationId" = $(integrationId) AND "connectionId" = $(connectionId) AND model = $(model)`,
    { integrationId, connectionId, model },
  )
  return row?.lastCheckedAt ?? null
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
        AND nc.model <> 'GithubStar'
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

export async function findActiveNangoMappingForRepo(
  qx: QueryExecutor,
  owner: string,
  repoName: string,
): Promise<INangoMappingRow | null> {
  return qx.selectOneOrNone(
    `
    SELECT nm.*
    FROM integration.nango_mapping nm
    JOIN integrations i ON i.id = nm."integrationId"
    WHERE lower(nm.owner) = lower($(owner))
      AND lower(nm."repoName") = lower($(repoName))
      AND i.platform = $(nangoPlatform)
      AND i."deletedAt" IS NULL
    ORDER BY nm."updatedAt" DESC
    LIMIT 1
    `,
    { owner, repoName, nangoPlatform: PlatformType.GITHUB_NANGO },
  )
}

export interface IUnmirroredNangoRepo {
  integrationId: string
  connectionId: string
  owner: string
  repoName: string
  enabled: boolean
  archived: boolean
  excluded: boolean
}

export async function listUnmirroredNangoRepos(qx: QueryExecutor): Promise<IUnmirroredNangoRepo[]> {
  return qx.select(
    `
    SELECT
      nm."integrationId",
      nm."connectionId",
      nm.owner,
      nm."repoName",
      COALESCE(r.enabled, true) AS enabled,
      COALESCE(r.archived, false) AS archived,
      COALESCE(r.excluded, false) AS excluded
    FROM integration.nango_mapping nm
    JOIN integrations i ON i.id = nm."integrationId"
    LEFT JOIN repositories r ON r.id = nm."repositoryId"
    WHERE i.platform = $(nangoPlatform)
      AND i."deletedAt" IS NULL
      AND i.status = 'done'
      AND NOT EXISTS (
        SELECT 1
        FROM integration.sync_units su
        WHERE su."integrationId" = nm."integrationId"
          AND lower(su."channelName") = lower('https://github.com/' || nm.owner || '/' || nm."repoName")
      )
    ORDER BY nm."createdAt", nm.owner, nm."repoName"
    `,
    { nangoPlatform: PlatformType.GITHUB_NANGO },
  )
}

export async function getNangoMappingForRepo(
  qx: QueryExecutor,
  integrationId: string,
  owner: string,
  repoName: string,
): Promise<INangoMappingRow | null> {
  const rows = await qx.select(
    `
    SELECT nm.*
    FROM integration.nango_mapping nm
    JOIN integrations nango_integration ON nango_integration.id = nm."integrationId"
    JOIN integrations new_integration ON new_integration."segmentId" = nango_integration."segmentId"
    WHERE new_integration.id = $(integrationId)
      AND nango_integration.platform = $(nangoPlatform)
      AND nango_integration."deletedAt" IS NULL
      AND LOWER(nm.owner) = LOWER($(owner))
      AND LOWER(nm."repoName") = LOWER($(repoName))
    ORDER BY nm."updatedAt" DESC
    LIMIT 1
    `,
    { integrationId, nangoPlatform: PlatformType.GITHUB_NANGO, owner, repoName },
  )

  return rows[0] ?? null
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

  const added = await qx.result(
    `
    update integrations
    set settings = jsonb_set(
          coalesce(settings, '{}'::jsonb),
          '{remotes}',
          coalesce(settings->'remotes', '[]'::jsonb) || to_jsonb($(repoUrl)::text)
        ),
        "updatedAt" = now()
    where id = $(id)
      and not (coalesce(settings->'remotes', '[]'::jsonb) ? $(repoUrl))
    `,
    { id: gitIntegration.id, repoUrl },
  )

  if (added === 0) {
    log.debug({ repoUrl }, 'Repo already exists in git integration, skipping!')
    return
  }

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

export interface IGithubLifecycleIntegration {
  id: string
  platform: string
  segmentId: string | null
  settings: IGithubIntegrationSettings | null
  deletedAt: string | null
}

export async function fetchGithubLifecycleIntegration(
  qx: QueryExecutor,
  id: string,
): Promise<IGithubLifecycleIntegration | null> {
  return qx.selectOneOrNone(
    `
    SELECT id, platform, "segmentId", settings, "deletedAt"
    FROM integrations
    WHERE id = $(id)
    `,
    { id },
  )
}

const NANGO_MAPPING_CHANNEL_KEY = `lower('https://github.com/' || nm.owner || '/' || nm."repoName")`

export async function listGithubChannelsOwnedElsewhere(
  qx: QueryExecutor,
  integrationId: string,
  channelKeys: string[],
): Promise<string[]> {
  if (channelKeys.length === 0) {
    return []
  }

  const rows: { channelKey: string }[] = await qx.select(
    `
    SELECT DISTINCT k.key AS "channelKey"
    FROM unnest($(channelKeys)::text[]) AS k(key)
    JOIN integration.sync_units su ON lower(su."channelName") = lower(k.key)
    JOIN integrations i
      ON i.id = su."integrationId"
     AND i.platform = $(nangoPlatform)
     AND i."deletedAt" IS NULL
    WHERE su."integrationId" <> $(integrationId)
    UNION
    SELECT DISTINCT k.key AS "channelKey"
    FROM unnest($(channelKeys)::text[]) AS k(key)
    JOIN integration.nango_mapping nm ON ${NANGO_MAPPING_CHANNEL_KEY} = lower(k.key)
    JOIN integrations i
      ON i.id = nm."integrationId"
     AND i.platform = $(nangoPlatform)
     AND i."deletedAt" IS NULL
    WHERE nm."integrationId" <> $(integrationId)
    `,
    { channelKeys, integrationId, nangoPlatform: PlatformType.GITHUB_NANGO },
  )
  return rows.map((row) => row.channelKey)
}

export async function listGithubRepoKeysUnderOwners(
  qx: QueryExecutor,
  owners: string[],
): Promise<string[]> {
  if (owners.length === 0) {
    return []
  }

  const rows: { key: string }[] = await qx.select(
    `
    WITH live_integrations AS (
      SELECT id
      FROM integrations
      WHERE platform = $(nangoPlatform)
        AND "deletedAt" IS NULL
    )
    SELECT lower(r.url) AS key
    FROM live_integrations li
    JOIN public.repositories r
      ON r."sourceIntegrationId" = li.id
     AND r."deletedAt" IS NULL
    WHERE r.url LIKE 'https://github.com/%'
      AND lower(split_part(r.url, '/', 4)) = ANY($(owners)::text[])
    UNION
    SELECT ${NANGO_MAPPING_CHANNEL_KEY} AS key
    FROM live_integrations li
    JOIN integration.nango_mapping nm ON nm."integrationId" = li.id
    WHERE lower(nm.owner) = ANY($(owners)::text[])
    `,
    { owners, nangoPlatform: PlatformType.GITHUB_NANGO },
  )
  return rows.map((row) => row.key)
}

export async function listGithubIntegrationsNeedingReconcile(qx: QueryExecutor): Promise<string[]> {
  const rows: { id: string }[] = await qx.select(
    `
    WITH nango_integrations AS (
      SELECT id, settings, "deletedAt"
      FROM integrations
      WHERE platform = $(nangoPlatform)
    ),
    live_integrations AS (
      SELECT id, settings
      FROM nango_integrations
      WHERE "deletedAt" IS NULL
    ),
    unit_keys AS (
      SELECT DISTINCT su."integrationId", lower(su."channelName") AS key
      FROM integration.sync_units su
      JOIN nango_integrations ni ON ni.id = su."integrationId"
    ),
    mapping_keys AS (
      SELECT DISTINCT nm."integrationId", ${NANGO_MAPPING_CHANNEL_KEY} AS key
      FROM integration.nango_mapping nm
      JOIN nango_integrations ni ON ni.id = nm."integrationId"
    ),
    repo_keys AS (
      SELECT DISTINCT r."sourceIntegrationId" AS "integrationId", lower(r.url) AS key
      FROM public.repositories r
      JOIN live_integrations li ON li.id = r."sourceIntegrationId"
      WHERE r."deletedAt" IS NULL
    ),
    unsynced_repos AS (
      SELECT rk."integrationId"
      FROM repo_keys rk
      LEFT JOIN unit_keys uk ON uk."integrationId" = rk."integrationId" AND uk.key = rk.key
      LEFT JOIN mapping_keys mk ON mk."integrationId" = rk."integrationId" AND mk.key = rk.key
      WHERE uk.key IS NULL AND mk.key IS NULL
    ),
    orphaned_units AS (
      SELECT uk."integrationId"
      FROM unit_keys uk
      JOIN live_integrations li ON li.id = uk."integrationId"
      LEFT JOIN repo_keys rk ON rk."integrationId" = uk."integrationId" AND rk.key = uk.key
      WHERE rk.key IS NULL
    ),
    deleted_with_units AS (
      SELECT ni.id AS "integrationId"
      FROM nango_integrations ni
      WHERE ni."deletedAt" IS NOT NULL
        AND ni.id IN (SELECT "integrationId" FROM unit_keys)
    ),
    deleted_with_repos AS (
      SELECT ni.id AS "integrationId"
      FROM nango_integrations ni
      WHERE ni."deletedAt" IS NOT NULL
        AND EXISTS (
          SELECT 1
          FROM public.repositories r
          WHERE r."sourceIntegrationId" = ni.id
            AND r."deletedAt" IS NULL
        )
    ),
    full_sync AS (
      SELECT li.id AS "integrationId"
      FROM live_integrations li
      WHERE jsonb_path_exists(li.settings, '$.orgs[*] ? (@.fullSync == true)')
    ),
    decommissioned_units AS (
      SELECT DISTINCT su."integrationId"
      FROM integration.sync_units su
      JOIN nango_integrations ni ON ni.id = su."integrationId"
      WHERE su.status = 'decommissioned'
    )
    SELECT "integrationId" AS id FROM unsynced_repos
    UNION
    SELECT "integrationId" FROM orphaned_units
    UNION
    SELECT "integrationId" FROM deleted_with_units
    UNION
    SELECT "integrationId" FROM deleted_with_repos
    UNION
    SELECT "integrationId" FROM full_sync
    UNION
    SELECT "integrationId" FROM decommissioned_units
    `,
    { nangoPlatform: PlatformType.GITHUB_NANGO },
  )
  return rows.map((row) => row.id)
}
