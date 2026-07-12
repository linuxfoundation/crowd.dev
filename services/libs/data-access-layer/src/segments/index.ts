import lodash from 'lodash'
import cloneDeep from 'lodash.clonedeep'

import { DEFAULT_TENANT_ID, generateUUIDv1 } from '@crowd/common'
import { DEFAULT_ACTIVITY_TYPE_SETTINGS } from '@crowd/integrations/src/integrations/activityTypes'
import {
  ActivityTypeSettings,
  MergeActionState,
  MergeActionType,
  PlatformType,
  SegmentData,
  SegmentDbInsert,
  SegmentDbRow,
  SegmentRawData,
  SegmentStatus,
} from '@crowd/types'

import { QueryExecutor } from '../queryExecutor'
import { prepareBulkInsert } from '../utils'

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

export async function insertSegments(
  qx: QueryExecutor,
  segments: SegmentDbInsert[],
  failOnConflict: boolean,
  returnRows: true,
): Promise<SegmentDbRow[]>
export async function insertSegments(
  qx: QueryExecutor,
  segments: SegmentDbInsert[],
  failOnConflict?: boolean,
  returnRows?: false,
): Promise<number>
export async function insertSegments(
  qx: QueryExecutor,
  segments: SegmentDbInsert[],
  failOnConflict = false,
  returnRows = false,
): Promise<SegmentDbRow[] | number> {
  const ts = new Date()

  const query = prepareBulkInsert(
    'segments',
    [
      'id',
      'slug',
      'name',
      'url',
      'status',
      'isLF',
      'parentName',
      'grandparentName',
      'parentSlug',
      'grandparentSlug',
      'description',
      'sourceId',
      'sourceParentId',
      'customActivityTypes',
      'activityChannels',
      'parentId',
      'grandparentId',
      // `type` is GENERATED ALWAYS from parentSlug/grandparentSlug — do not insert
      'maturity',
      'tenantId',
      'createdAt',
      'updatedAt',
    ],
    segments.map((s) => ({
      id: s.id ?? generateUUIDv1(),
      slug: s.slug,
      name: s.name ?? null,
      url: s.url ?? null,
      status: s.status ?? SegmentStatus.ACTIVE,
      isLF: s.isLF ?? true,
      parentName: s.parentName ?? null,
      grandparentName: s.grandparentName ?? null,
      parentSlug: s.parentSlug ?? null,
      grandparentSlug: s.grandparentSlug ?? null,
      description: s.description ?? null,
      sourceId: s.sourceId ?? null,
      sourceParentId: s.sourceParentId ?? null,
      customActivityTypes: s.customActivityTypes ?? {},
      activityChannels: s.activityChannels ?? {},
      parentId: s.parentId ?? null,
      grandparentId: s.grandparentId ?? null,
      maturity: s.maturity ?? null,
      tenantId: DEFAULT_TENANT_ID,
      createdAt: ts,
      updatedAt: ts,
    })),
    failOnConflict ? undefined : 'DO NOTHING',
    returnRows,
  )

  if (returnRows) {
    return qx.select(query)
  }

  return qx.result(query)
}

export async function getSegmentSubprojects(
  qx: QueryExecutor,
  segmentIds: string[],
): Promise<SegmentRawData[]> {
  if (segmentIds.length === 0) {
    return []
  }

  return qx.select(
    `
      with input_segment AS (
        select
          id,
          slug,
          "parentSlug",
          "grandparentSlug"
        from segments
        where id = ANY($(segmentIds)::UUID[])
          and "tenantId" = $(tenantId)
      ),
      segment_level AS (
        select
          case
            when "parentSlug" is not null and "grandparentSlug" is not null
                then 'child'
            when "parentSlug" is not null and "grandparentSlug" is null
                then 'parent'
            when "parentSlug" is null and "grandparentSlug" is null
                then 'grandparent'
            end as level,
          id,
          slug,
          "parentSlug",
          "grandparentSlug"
        from input_segment
      )
        select distinct s.*
        from segments s
        join segment_level sl on (sl.level = 'child' and s.id = sl.id)
            or (sl.level = 'parent' and s."parentSlug" = sl.slug and s."grandparentSlug" = sl."parentSlug")
            or (sl.level = 'grandparent' and s."grandparentSlug" = sl.slug)
        where status = 'active'
          and s."tenantId" = $(tenantId);
    `,
    {
      tenantId: DEFAULT_TENANT_ID,
      segmentIds,
    },
  )
}

export async function getSegmentSubprojectIds(
  qx: QueryExecutor,
  segmentIds: string[],
): Promise<string[]> {
  const subprojects = await getSegmentSubprojects(qx, segmentIds)
  return subprojects.map((s) => s.id)
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

export async function fetchProjectGroupSegmentIds(qx: QueryExecutor): Promise<string[]> {
  const rows: { id: string }[] = await qx.select(
    `
      SELECT id
      FROM segments
      WHERE "parentId" IS NULL
        AND "grandparentId" IS NULL
        AND "tenantId" = $(tenantId)
        AND status = 'active'
      ORDER BY id
    `,
    {
      tenantId: DEFAULT_TENANT_ID,
    },
  )

  return rows.map((row) => row.id)
}

export async function calculateSegmentMemberMergeSuggestionsCount(
  qx: QueryExecutor,
  segmentId: string,
): Promise<number> {
  const result = await qx.selectOne(
    `
      SELECT COUNT(*) AS count
      FROM "memberToMerge" mtm
      WHERE EXISTS (
        SELECT 1
        FROM "memberSegmentsAgg" ms
        WHERE ms."memberId" = mtm."memberId"
          AND ms."segmentId" = $(segmentId)
      )
      AND EXISTS (
        SELECT 1
        FROM "memberSegmentsAgg" ms2
        WHERE ms2."memberId" = mtm."toMergeId"
          AND ms2."segmentId" = $(segmentId)
      )
      AND NOT EXISTS (
        SELECT 1
        FROM "mergeActions" ma
        WHERE ma.type = $(mergeActionType)
          AND ma.state <> $(mergeActionState)
          AND (
            (ma."primaryId" = mtm."memberId" AND ma."secondaryId" = mtm."toMergeId")
            OR (ma."primaryId" = mtm."toMergeId" AND ma."secondaryId" = mtm."memberId")
          )
      )
    `,
    {
      segmentId,
      mergeActionType: MergeActionType.MEMBER,
      mergeActionState: MergeActionState.ERROR,
    },
  )

  return Number(result.count)
}

export async function calculateSegmentOrganizationMergeSuggestionsCount(
  qx: QueryExecutor,
  segmentId: string,
): Promise<number> {
  const result = await qx.selectOne(
    `
      SELECT COUNT(*) AS count
      FROM "organizationToMerge" otm
      WHERE EXISTS (
        SELECT 1
        FROM "organizationSegmentsAgg" os1
        WHERE os1."organizationId" = otm."organizationId"
          AND os1."segmentId" = $(segmentId)
      )
      AND EXISTS (
        SELECT 1
        FROM "organizationSegmentsAgg" os2
        WHERE os2."organizationId" = otm."toMergeId"
          AND os2."segmentId" = $(segmentId)
      )
      AND NOT EXISTS (
        SELECT 1
        FROM "mergeActions" ma
        WHERE ma.type = $(mergeActionType)
          AND ma.state <> $(mergeActionState)
          AND (
            (ma."primaryId" = otm."organizationId" AND ma."secondaryId" = otm."toMergeId")
            OR (ma."primaryId" = otm."toMergeId" AND ma."secondaryId" = otm."organizationId")
          )
      )
    `,
    {
      segmentId,
      mergeActionType: MergeActionType.ORG,
      mergeActionState: MergeActionState.ERROR,
    },
  )

  return Number(result.count)
}

interface SegmentMergeSuggestionCounts {
  memberMergeSuggestionsCount: number
  organizationMergeSuggestionsCount: number
}

export async function upsertSegmentMergeSuggestionCounts(
  qx: QueryExecutor,
  segmentId: string,
  data: SegmentMergeSuggestionCounts,
): Promise<void> {
  const { memberMergeSuggestionsCount, organizationMergeSuggestionsCount } = data

  await qx.result(
    `
      INSERT INTO "segmentMergeSuggestionCounts" (
        "segmentId",
        "memberMergeSuggestionsCount",
        "organizationMergeSuggestionsCount",
        "updatedAt"
      )
      VALUES (
        $(segmentId),
        $(memberMergeSuggestionsCount),
        $(organizationMergeSuggestionsCount),
        now()
      )
      ON CONFLICT ("segmentId")
      DO UPDATE SET
        "memberMergeSuggestionsCount" = EXCLUDED."memberMergeSuggestionsCount",
        "organizationMergeSuggestionsCount" = EXCLUDED."organizationMergeSuggestionsCount",
        "updatedAt" = EXCLUDED."updatedAt"
    `,
    {
      segmentId,
      memberMergeSuggestionsCount,
      organizationMergeSuggestionsCount,
    },
  )
}

export async function getSegmentMergeSuggestionCounts(
  qx: QueryExecutor,
  segmentId: string,
): Promise<SegmentMergeSuggestionCounts | null> {
  const result = await qx.selectOneOrNone(
    `
      SELECT
        "memberMergeSuggestionsCount",
        "organizationMergeSuggestionsCount"
      FROM "segmentMergeSuggestionCounts"
      WHERE "segmentId" = $(segmentId)
    `,
    {
      segmentId,
    },
  )

  if (!result) {
    return null
  }

  return {
    memberMergeSuggestionsCount: Number(result.memberMergeSuggestionsCount),
    organizationMergeSuggestionsCount: Number(result.organizationMergeSuggestionsCount),
  }
}

export async function getMembersCommonProjectGroupSegmentIds(
  qx: QueryExecutor,
  memberIds: string[],
): Promise<string[]> {
  if (!memberIds || memberIds.length !== 2) {
    throw new Error('Exactly two memberIds are required')
  }

  const [memberId, otherMemberId] = memberIds

  const rows: { segmentId: string }[] = await qx.select(
    `
      SELECT DISTINCT ms."segmentId"
      FROM "memberSegmentsAgg" ms
      INNER JOIN "memberSegmentsAgg" ms2
        ON ms."segmentId" = ms2."segmentId"
      INNER JOIN segments s
        ON s.id = ms."segmentId"
      WHERE ms."memberId" = $(memberId)
        AND ms2."memberId" = $(otherMemberId)
        AND s."parentId" IS NULL
        AND s."grandparentId" IS NULL
        AND s."tenantId" = $(tenantId)
        AND s.status = 'active'
    `,
    {
      memberId,
      otherMemberId,
      tenantId: DEFAULT_TENANT_ID,
    },
  )

  return rows.map((row) => row.segmentId)
}

export async function getOrganizationsCommonProjectGroupSegmentIds(
  qx: QueryExecutor,
  organizationIds: string[],
): Promise<string[]> {
  if (!organizationIds || organizationIds.length !== 2) {
    throw new Error('Exactly two organizationIds are required')
  }

  const [organizationId, otherOrganizationId] = organizationIds

  const rows: { segmentId: string }[] = await qx.select(
    `
      SELECT DISTINCT os1."segmentId"
      FROM "organizationSegmentsAgg" os1
      INNER JOIN "organizationSegmentsAgg" os2
        ON os1."segmentId" = os2."segmentId"
      INNER JOIN segments s
        ON s.id = os1."segmentId"
      WHERE os1."organizationId" = $(organizationId)
        AND os2."organizationId" = $(otherOrganizationId)
        AND s."parentId" IS NULL
        AND s."grandparentId" IS NULL
        AND s."tenantId" = $(tenantId)
        AND s.status = 'active'
    `,
    {
      organizationId,
      otherOrganizationId,
      tenantId: DEFAULT_TENANT_ID,
    },
  )

  return rows.map((row) => row.segmentId)
}

export async function decrementMemberMergeSuggestionCounts(
  qx: QueryExecutor,
  segmentIds: string[],
): Promise<void> {
  if (segmentIds.length === 0) {
    return
  }

  await qx.result(
    `
      UPDATE "segmentMergeSuggestionCounts"
      SET
        "memberMergeSuggestionsCount" = GREATEST(0, "memberMergeSuggestionsCount" - 1),
        "updatedAt" = now()
      WHERE "segmentId" IN ($(segmentIds:csv))
    `,
    {
      segmentIds,
    },
  )
}

export async function decrementOrganizationMergeSuggestionCounts(
  qx: QueryExecutor,
  segmentIds: string[],
): Promise<void> {
  if (segmentIds.length === 0) {
    return
  }

  await qx.result(
    `
      UPDATE "segmentMergeSuggestionCounts"
      SET
        "organizationMergeSuggestionsCount" = GREATEST(0, "organizationMergeSuggestionsCount" - 1),
        "updatedAt" = now()
      WHERE "segmentId" IN ($(segmentIds:csv))
    `,
    {
      segmentIds,
    },
  )
}
