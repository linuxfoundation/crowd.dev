import lodash from 'lodash'
import cloneDeep from 'lodash.clonedeep'

import { DEFAULT_ACTIVITY_TYPE_SETTINGS } from '@crowd/integrations'
import { ActivityTypeSettings, PlatformType, SegmentData, SegmentRawData } from '@crowd/types'

import { QueryExecutor } from '../queryExecutor'

export async function findProjectGroupByName(
  qx: QueryExecutor,
  { name }: { name: string },
): Promise<SegmentData> {
  return qx.selectOneOrNone(
    `
      SELECT *
      FROM segments
      WHERE name = $(name)
        AND "parentSlug" IS NULL
        AND "grandparentSlug" IS NULL
    `,
    { name },
  )
}

export async function findLfSegmentByName(
  qx: QueryExecutor,
  name: string,
): Promise<SegmentData | null> {
  return qx.selectOneOrNone(
    `
      SELECT *
      FROM segments
      WHERE "isLF" = true
        AND trim(lower(name)) = trim(lower($(name)))
      LIMIT 1;
    `,
    { name },
  )
}

export async function fetchManySegments(
  qx,
  segmentIds: string[],
  fields = '*',
): Promise<SegmentData[]> {
  return qx.select(
    `
      SELECT ${fields}
      FROM segments
      WHERE id = ANY($(segmentIds)::UUID[])
    `,
    { segmentIds },
  )
}

export async function findSegmentById(
  qx: QueryExecutor,
  segmentId: string,
): Promise<SegmentData | null> {
  const record = await qx.selectOneOrNone(
    `
      SELECT *
      FROM segments
      WHERE id = $(segmentId)
    `,
    { segmentId },
  )

  if (!record) {
    return null
  }

  if (isSegmentProjectGroup(record)) {
    // find projects
    // TODO: Check sorting - parent should come first
    const children = await getSegmentChildrenOfProjectGroups(qx, record)

    const projects = children.reduce((acc, child) => {
      if (isSegmentProject(child)) {
        acc.push(child)
      } else if (isSegmentSubproject(child)) {
        // find project index
        const projectIndex = acc.findIndex((project) => project.slug === child.parentSlug)
        // process subproject only if its parent project exists
        if (projectIndex !== -1) {
          if (!acc[projectIndex].subprojects) {
            acc[projectIndex].subprojects = [child]
          } else {
            acc[projectIndex].subprojects.push(child)
          }
        }
      }
      return acc
    }, [])

    record.projects = projects
  } else if (isSegmentProject(record)) {
    const children = await getSegmentChildrenOfProjects(qx, record)
    record.subprojects = children
  }

  return record
}

export async function getSegmentChildrenOfProjectGroups(
  qx: QueryExecutor,
  segment: SegmentData,
): Promise<SegmentRawData[]> {
  const records = await qx.select(
    `
    select * from segments s
    where (s."grandparentSlug" = $(slug) or
                 (s."parentSlug" = $(slug) and s."grandparentSlug" is null))
          order by s."grandparentSlug" desc, s."parentSlug" desc, s.slug desc;
    `,
    {
      slug: segment.slug,
    },
  )

  return records
}

export async function getSegmentChildrenOfProjects(
  qx: QueryExecutor,
  segment: SegmentData,
): Promise<SegmentRawData[]> {
  const records = await qx.select(
    `
    select * from segments s
      where s."parentSlug" = $(slug)
        AND s."grandparentSlug" = $(parentSlug)
    `,
    {
      slug: segment.slug,
      parentSlug: segment.parentSlug,
    },
  )

  return records
}

export function getSegmentActivityTypes(segments: SegmentData[]): ActivityTypeSettings {
  return segments.reduce((acc, s) => lodash.merge(acc, s.activityTypes), {})
}

export function isSegmentProjectGroup(segment: SegmentData | SegmentRawData): boolean {
  return segment.slug && segment.parentSlug === null && segment.grandparentSlug === null
}

export function isSegmentProject(segment: SegmentData | SegmentRawData): boolean {
  return segment.slug && segment.parentSlug && segment.grandparentSlug === null
}

export function isSegmentSubproject(segment: SegmentData | SegmentRawData): boolean {
  return segment.slug != null && segment.parentSlug != null && segment.grandparentSlug != null
}

export function buildSegmentActivityTypes(segment: SegmentRawData): ActivityTypeSettings {
  const activityTypes = {} as ActivityTypeSettings

  activityTypes.default = cloneDeep(DEFAULT_ACTIVITY_TYPE_SETTINGS)
  activityTypes.custom = {}

  const customActivityTypes = segment.customActivityTypes || {}

  if (Object.keys(customActivityTypes).length > 0) {
    activityTypes.custom = customActivityTypes
  }

  return activityTypes
}

export function populateSegmentRelations(record: SegmentRawData): SegmentData {
  const segmentData: SegmentData = {
    ...record,
    activityTypes: null,
  }

  if (isSegmentSubproject(record)) {
    segmentData.activityTypes = buildSegmentActivityTypes(record)
  }

  return segmentData
}

export async function getMappedRepos(
  qx: QueryExecutor,
  segmentId: string,
  platform: PlatformType,
): Promise<Array<{ url: string }>> {
  // GIT mirrors repos from other platforms, so use gitIntegrationId; otherwise use sourceIntegrationId
  const integrationJoinColumn =
    platform === PlatformType.GIT ? 'gitIntegrationId' : 'sourceIntegrationId'

  return qx.select(
    `
      SELECT
        r.url as url
      FROM
        public.repositories r
      JOIN
        integrations i ON r."${integrationJoinColumn}" = i.id
      WHERE r."segmentId" = $(segmentId)
        AND i.platform = $(platform)
        AND r."deletedAt" IS NULL
      ORDER BY r.url
    `,
    { segmentId, platform },
  )
}

export interface IRepoByPlatform {
  url: string
  platform: string
  enabled: boolean
}

/**
 * Get all repositories for a segment, grouped by platform.
 * Joins with the integrations table to determine the platform for each repo.
 *
 * @param qx - Query executor
 * @param segmentId - The segment ID to get repos for
 * @param mergeGithubNango - If true, merges 'github-nango' platform into 'github' (default: true)
 * @returns Record of platform -> array of repo objects with url and enabled status
 */
export async function getReposBySegmentGroupedByPlatform(
  qx: QueryExecutor,
  segmentId: string,
  mergeGithubNango = true,
): Promise<Record<string, Array<{ url: string; enabled: boolean }>>> {
  const rows: IRepoByPlatform[] = await qx.select(
    `
      SELECT DISTINCT
        r.url,
        i.platform,
        r.enabled
      FROM public.repositories r
      JOIN integrations i ON r."sourceIntegrationId" = i.id
      WHERE r."segmentId" = $(segmentId)
        AND r."deletedAt" IS NULL
        AND i."deletedAt" IS NULL
      ORDER BY i.platform, r.url
    `,
    { segmentId },
  )

  const result: Record<string, Array<{ url: string; enabled: boolean }>> = {}

  for (const row of rows) {
    let platform = row.platform

    // Merge github-nango into github if requested
    if (mergeGithubNango && platform === PlatformType.GITHUB_NANGO) {
      platform = PlatformType.GITHUB
    }

    if (!result[platform]) {
      result[platform] = []
    }

    result[platform].push({ url: row.url, enabled: row.enabled })
  }

  return result
}

export async function getRepoUrlsMappedToOtherSegments(
  qx: QueryExecutor,
  urls: string[],
  segmentId: string,
): Promise<string[]> {
  if (!urls || urls.length === 0) {
    return []
  }

  const rows = await qx.select(
    `
      SELECT DISTINCT
        r.url as url
      FROM
        public.repositories r
      WHERE
        r.url = ANY($(urls)::text[])
        AND r."deletedAt" IS NULL
        AND r."segmentId" <> $(segmentId)
    `,
    { urls, segmentId },
  )

  return rows.map((r: { url: string }) => r.url)
}

export async function hasMappedRepos(
  qx: QueryExecutor,
  segmentId: string,
  platforms: PlatformType[],
): Promise<boolean> {
  if (platforms.length === 0) {
    return false
  }

  const result = await qx.selectOneOrNone(
    `
      SELECT EXISTS (
        SELECT 1
        FROM public.repositories r
        LEFT JOIN integrations i ON r."sourceIntegrationId" = i.id
        WHERE r."segmentId" = $(segmentId)
          AND r."deletedAt" IS NULL
          AND (
            i.id IS NULL
            OR (i.platform = ANY($(platforms)::text[]) AND i."segmentId" <> $(segmentId))
          )
        LIMIT 1
      ) as has_repos
    `,
    { segmentId, platforms },
  )

  return result?.has_repos ?? false
}

export async function getMappedWithSegmentName(
  qx: QueryExecutor,
  segmentId: string,
  platforms: PlatformType[],
): Promise<string | null> {
  if (platforms.length === 0) {
    return null
  }

  const result = await qx.selectOneOrNone(
    `
      SELECT s.name as segment_name
      FROM public.repositories r
      LEFT JOIN integrations i ON r."sourceIntegrationId" = i.id
      LEFT JOIN segments s ON i."segmentId" = s.id
      WHERE r."segmentId" = $(segmentId)
        AND r."deletedAt" IS NULL
        AND (
          i.id IS NULL
          OR (i.platform = ANY($(platforms)::text[]) AND i."segmentId" <> $(segmentId))
        )
      LIMIT 1
    `,
    { segmentId, platforms },
  )

  return result?.segment_name ?? null
}

export async function getMappedAllWithSegmentName(
  qx: QueryExecutor,
  segmentId: string,
  platforms: PlatformType[],
): Promise<{ segmentName: string; url: string }[]> {
  if (platforms.length === 0) {
    return []
  }

  const results = await qx.select(
    `
      SELECT DISTINCT s.name as segment_name, r.url
      FROM public.repositories r
      LEFT JOIN integrations i ON r."sourceIntegrationId" = i.id
      LEFT JOIN segments s ON i."segmentId" = s.id
      WHERE r."segmentId" = $(segmentId)
        AND r."deletedAt" IS NULL
        AND (
          i.id IS NULL
          OR (i.platform = ANY($(platforms)::text[]) AND i."segmentId" <> $(segmentId))
        )
    `,
    { segmentId, platforms },
  )

  return results
    .filter((r: { segment_name: string; url: string }) => r.segment_name)
    .map((r: { segment_name: string; url: string }) => ({
      segmentName: r.segment_name,
      url: r.url,
    }))
}

export interface ISegment {
  id: string
  name: string
  parentId: string | null
  grandparentId: string | null
}

// Using Record instead of Map for JSON serialization compatibility with Temporal
export interface ISegmentHierarchy {
  projectSegments: ISegment[]
  projectGroupSegments: ISegment[]
  subprojectsByParent: Record<string, string[]>
  subprojectsByGrandparent: Record<string, string[]>
  segmentNames: Record<string, string>
  projectToProjectGroup: Record<string, string>
}

/**
 * Get segment hierarchy with all projects, project groups, and their relationships.
 * Used for aggregate calculation to determine which subprojects roll up to which projects/project groups.
 */
export async function getSegmentHierarchy(qx: QueryExecutor): Promise<ISegmentHierarchy> {
  const segments: ISegment[] = await qx.select(
    `
    SELECT id, name, "parentId", "grandparentId"
    FROM segments
    `,
  )

  const segmentNames: Record<string, string> = {}
  const projectToProjectGroup: Record<string, string> = {}
  const subprojectsByParent: Record<string, string[]> = {}
  const subprojectsByGrandparent: Record<string, string[]> = {}

  // Build segment name lookup and project -> project group mapping
  for (const s of segments) {
    segmentNames[s.id] = s.name
    // Projects have parentId (pointing to project group) but no grandparentId
    if (s.parentId !== null && s.grandparentId === null) {
      projectToProjectGroup[s.id] = s.parentId
    }
  }

  // Separate segments by type
  const projectSegments = segments.filter((s) => s.parentId !== null && s.grandparentId === null)
  const projectGroupSegments = segments.filter(
    (s) => s.parentId === null && s.grandparentId === null,
  )
  const subprojectSegments = segments.filter((s) => s.parentId !== null && s.grandparentId !== null)

  // Build mappings: which subprojects belong to which parent/grandparent
  for (const sp of subprojectSegments) {
    // Map to parent (project)
    if (sp.parentId) {
      if (!subprojectsByParent[sp.parentId]) {
        subprojectsByParent[sp.parentId] = []
      }
      subprojectsByParent[sp.parentId].push(sp.id)
    }

    // Map to grandparent (project group)
    if (sp.grandparentId) {
      if (!subprojectsByGrandparent[sp.grandparentId]) {
        subprojectsByGrandparent[sp.grandparentId] = []
      }
      subprojectsByGrandparent[sp.grandparentId].push(sp.id)
    }
  }

  return {
    projectSegments,
    projectGroupSegments,
    subprojectsByParent,
    subprojectsByGrandparent,
    segmentNames,
    projectToProjectGroup,
  }
}
export async function getSubProjectsCount(
  qx: QueryExecutor,
  segmentId?: string,
): Promise<{ projectsTotal: number; projectsLast30Days: number }> {
  let query: string
  let params: Record<string, string>

  if (!segmentId) {
    // Count only subprojects (segments with both parentSlug and grandparentSlug)
    query = `
      SELECT 
        COUNT(*) as "projectsTotal",
        COUNT(CASE WHEN "createdAt" >= NOW() - INTERVAL '30 days' THEN 1 END) as "projectsLast30Days"
      FROM segments 
      WHERE type = 'subproject'
    `
    params = {}
  } else {
    // Count only subprojects regardless of the filter being applied (project group or project)
    query = `
      SELECT 
        COUNT(*) as "projectsTotal",
        COUNT(CASE WHEN s."createdAt" >= NOW() - INTERVAL '30 days' THEN 1 END) as "projectsLast30Days"
      FROM segments s
      WHERE type = 'subproject'
        AND (s.id = $(segmentId) OR s."parentId" = $(segmentId) OR s."grandparentId" = $(segmentId))
    `
    params = { segmentId }
  }

  const [result] = await qx.select(query, params)
  return {
    projectsTotal: parseInt(result.projectsTotal) || 0,
    projectsLast30Days: parseInt(result.projectsLast30Days) || 0,
  }
}
