import { uniq } from 'lodash'

import {
  DEFAULT_TENANT_ID,
  Error400,
  RawQueryParser,
  generateUUIDv1,
  getProperDisplayName,
  groupBy,
} from '@crowd/common'
import { formatSql, getDbInstance, prepareForModification } from '@crowd/database'
import { getServiceLogger } from '@crowd/logging'
import { RedisClient } from '@crowd/redis'
import {
  ALL_PLATFORM_TYPES,
  IMemberContribution,
  MemberAttributeType,
  MemberRow,
  PageData,
  SegmentType,
} from '@crowd/types'

import { findMaintainerRoles } from '../maintainers'
import { QueryExecutor } from '../queryExecutor'
import { fetchManySegments } from '../segments'
import { QueryOptions, QueryResult, queryTable, queryTableById } from '../utils'

import { getMemberAttributeSettings } from './attributeSettings'
import { fetchOrganizationData, fetchSegmentData, sortActiveOrganizations } from './dataProcessor'
import { buildCountQuery, buildQuery, buildSearchCTE } from './queryBuilder'
import { MemberQueryCache } from './queryCache'
import { IDbMemberAttributeSetting, IDbMemberData } from './types'

import { fetchManyMemberIdentities, fetchManyMemberOrgs, fetchManyMemberSegments } from '.'

const log = getServiceLogger()

interface IQueryMembersAdvancedParams {
  filter?: Record<string, unknown>
  search?: string | null
  limit?: number
  offset?: number
  orderBy?: string
  segmentId?: string
  countOnly?: boolean
  fields?: string[]
  includeAllAttributes?: boolean
  include?: {
    identities?: boolean
    segments?: boolean
    lfxMemberships?: boolean
    memberOrganizations?: boolean
    onlySubProjects?: boolean
    maintainers?: boolean
  }
  attributeSettings?: IDbMemberAttributeSetting[]
}

export enum MemberField {
  ATTRIBUTES = 'attributes',
  CONTRIBUTIONS = 'contributions',
  CREATED_AT = 'createdAt',
  CREATED_BY_ID = 'createdById',
  DELETED_AT = 'deletedAt',
  DISPLAY_NAME = 'displayName',
  ID = 'id',
  IMPORT_HASH = 'importHash',
  JOINED_AT = 'joinedAt',
  MANUALLY_CHANGED_FIELDS = 'manuallyChangedFields',
  MANUALLY_CREATED = 'manuallyCreated',
  REACH = 'reach',
  SCORE = 'score',
  TENANT_ID = 'tenantId',
  UPDATED_AT = 'updatedAt',
  UPDATED_BY_ID = 'updatedById',
}

export interface MemberCreateInput {
  id?: string
  displayName: string
  joinedAt: string
  attributes: Record<string, unknown>
  reach: Partial<Record<string, number>>
  manuallyCreated?: boolean
  contributions?: IMemberContribution[]
}

export interface MemberUpdateInput {
  joinedAt?: string
  attributes?: Record<string, unknown>
  displayName?: string
  reach?: Partial<Record<string, number>>
  contributions?: IMemberContribution[] | string
  manuallyChangedFields?: string[]
  manuallyCreated?: boolean
}

export const BLACKLISTED_MEMBER_TITLES = ['investor', 'mentor', 'board member']

export const MEMBER_MERGE_FIELDS = [
  'affiliations',
  'attributes',
  'contributions',
  'displayName',
  'id',
  'joinedAt',
  'manuallyChangedFields',
  'manuallyCreated',
  'reach',
]

export const MEMBER_UPDATE_COLUMNS = [
  MemberField.ATTRIBUTES,
  MemberField.CONTRIBUTIONS,
  MemberField.DISPLAY_NAME,
  MemberField.IMPORT_HASH,
  MemberField.REACH,
  MemberField.SCORE,
]

export const MEMBER_SELECT_COLUMNS = [
  'attributes',
  'displayName',
  'id',
  'joinedAt',
  'manuallyChangedFields',
  'reach',
  'score',
]

export const MEMBER_INSERT_COLUMNS = [
  'attributes',
  'contributions',
  'createdAt',
  'displayName',
  'id',
  'joinedAt',
  'manuallyCreated',
  'reach',
  'tenantId',
  'updatedAt',
]

const QUERY_FILTER_COLUMN_MAP: Map<string, { name: string; queryable?: boolean }> = new Map([
  ['activityCount', { name: 'msa."activityCount"' }],
  ['attributes', { name: 'm.attributes' }],
  ['averageSentiment', { name: 'coalesce(msa."averageSentiment", 0)::decimal' }],
  ['displayName', { name: 'm."displayName"' }],
  ['id', { name: 'm.id' }],
  ['identityPlatforms', { name: 'coalesce(msa."activeOn", \'{}\'::text[])' }],
  ['isBot', { name: `COALESCE((m.attributes -> 'isBot' ->> 'default')::BOOLEAN, FALSE)` }],
  [
    'isOrganization',
    { name: `COALESCE((m.attributes -> 'isOrganization' ->> 'default')::BOOLEAN, FALSE)` },
  ],
  ['joinedAt', { name: 'm."joinedAt"' }],
  ['lastEnrichedAt', { name: 'me."lastUpdatedAt"' }],
  ['organizations', { name: 'mo."organizationId"', queryable: false }],
  ['score', { name: 'm.score' }],
  ['segmentId', { name: 'msa."segmentId"' }],
])

export async function queryMembersAdvanced(
  qx: QueryExecutor,
  bgQx: QueryExecutor,
  redis: RedisClient,
  {
    filter = {},
    search = null,
    limit = 20,
    offset = 0,
    orderBy = 'activityCount_DESC',
    segmentId = undefined,
    countOnly = false,
    fields = [...QUERY_FILTER_COLUMN_MAP.keys()],
    includeAllAttributes = false,
    include = {
      identities: true,
      segments: false,
      lfxMemberships: false,
      memberOrganizations: false,
      onlySubProjects: false,
      maintainers: true,
    } as {
      identities?: boolean
      segments?: boolean
      lfxMemberships?: boolean
      memberOrganizations?: boolean
      onlySubProjects?: boolean
      maintainers?: boolean
    },
    attributeSettings = [] as IDbMemberAttributeSetting[],
  },
): Promise<PageData<IDbMemberData>> {
  // Initialize cache
  const cache = new MemberQueryCache(redis)

  // Normalize search once: trim whitespace, lowercase (buildSearchCTE lowercases anyway),
  // convert empty string to null so "" and null hash identically.
  const normalizedSearch = search?.trim().toLowerCase() || null

  // Full result key — includes pagination and projection so page 1 and page 2 are separate entries.
  const cacheKey = cache.buildCacheKey({
    fields,
    filter,
    include,
    includeAllAttributes,
    limit,
    offset,
    orderBy,
    search: normalizedSearch,
    segmentId,
  })

  // Count key — excludes pagination/projection and include flags since the count query
  // only depends on filter, search, and segmentId (buildCountQuery hardcodes includeMemberOrgs=false).
  const countCacheKey = cache.buildCountCacheKey({
    filter,
    search: normalizedSearch,
    segmentId,
  })

  // Try to get from cache first
  const cachedResult = countOnly ? null : await cache.get(cacheKey)
  const cachedCount = countOnly ? await cache.getCount(countCacheKey) : null

  if (cachedResult) {
    log.info(
      { cacheKey, segmentId, search: normalizedSearch, limit, offset, orderBy },
      'Members advanced query cache hit — returning cached result, scheduling background refresh',
    )
    refreshCacheInBackground(bgQx, redis, cacheKey, {
      filter,
      search: normalizedSearch,
      limit,
      offset,
      orderBy,
      segmentId,
      countOnly: false,
      fields,
      include,
      includeAllAttributes,
      attributeSettings,
    })
    return cachedResult
  }

  if (countOnly && cachedCount !== null) {
    log.info(
      { countCacheKey, segmentId, search: normalizedSearch },
      'Members advanced count cache hit — returning cached count',
    )
    // No background refresh for count hits: the count is a single integer with a 6h TTL,
    // refreshing it on every hit would fire a COUNT(*) query per request, defeating the cache.
    // The count is kept fresh by: (1) full-result refreshes that also write countCacheKey,
    // (2) natural TTL expiry, (3) explicit cache invalidation on member updates.
    return {
      rows: [],
      count: cachedCount,
      limit,
      offset,
    }
  }

  log.info(
    {
      cacheKey,
      countCacheKey,
      segmentId,
      search: normalizedSearch,
      limit,
      offset,
      orderBy,
      countOnly,
    },
    'Members advanced query cache miss — executing query synchronously',
  )

  try {
    return await executeQuery(qx, redis, cacheKey, {
      filter,
      search: normalizedSearch,
      limit,
      offset,
      orderBy,
      segmentId,
      countOnly,
      fields,
      include,
      includeAllAttributes,
      attributeSettings,
    })
  } catch (error) {
    log.warn(
      { cacheKey, countCacheKey, segmentId, search: normalizedSearch, countOnly, err: error },
      'Members advanced query failed on cache miss — scheduling background refresh for next retry',
    )
    if (countOnly) {
      refreshCountCacheInBackground(bgQx, redis, countCacheKey, {
        filter,
        search: normalizedSearch,
        segmentId,
        include,
        includeAllAttributes,
        attributeSettings,
      })
    } else {
      refreshCacheInBackground(bgQx, redis, cacheKey, {
        filter,
        search: normalizedSearch,
        limit,
        offset,
        orderBy,
        segmentId,
        countOnly: false,
        fields,
        include,
        includeAllAttributes,
        attributeSettings,
      })
    }
    throw error
  }
}

export async function executeQuery(
  qx: QueryExecutor,
  redis: RedisClient,
  cacheKey: string,
  {
    filter = {},
    search = null,
    limit = 20,
    offset = 0,
    orderBy = 'activityCount_DESC',
    segmentId = undefined,
    countOnly = false,
    fields = [...QUERY_FILTER_COLUMN_MAP.keys()],
    includeAllAttributes = false,
    include = {
      identities: true,
      segments: false,
      lfxMemberships: false,
      memberOrganizations: false,
      onlySubProjects: false,
      maintainers: true,
    } as {
      identities?: boolean
      segments?: boolean
      lfxMemberships?: boolean
      memberOrganizations?: boolean
      onlySubProjects?: boolean
      maintainers?: boolean
    },
    attributeSettings = [] as IDbMemberAttributeSetting[],
  }: IQueryMembersAdvancedParams,
): Promise<PageData<IDbMemberData>> {
  const cache = new MemberQueryCache(redis)
  const countCacheKey = cache.buildCountCacheKey({
    filter,
    search: search ?? null,
    segmentId,
  })
  const withAggregates = !!segmentId
  const searchConfig = buildSearchCTE(search)

  const params = {
    limit,
    offset,
    segmentId,
    ...searchConfig.params,
  }

  const filterString = RawQueryParser.parseFilters(
    filter,
    new Map([...QUERY_FILTER_COLUMN_MAP.entries()].map(([key, { name }]) => [key, name])),
    [
      {
        property: 'attributes',
        column: 'm.attributes',
        attributeInfos: [
          ...(attributeSettings?.length > 0
            ? attributeSettings
            : await getMemberAttributeSettings(qx, redis)),
          {
            name: 'jobTitle',
            type: MemberAttributeType.STRING,
          },
        ],
      },
      {
        property: 'username',
        column: 'aggs.username',
        attributeInfos: ALL_PLATFORM_TYPES.map((name) => ({
          name,
          type: MemberAttributeType.STRING,
        })),
      },
    ],
    params,
    { pgPromiseFormat: true },
  )

  const countQuery = buildCountQuery({
    withAggregates,
    searchConfig,
    filterString,
    // Count never needs org data in SELECT — filterHasMo inside buildCountQuery already
    // handles the case where the filter itself references mo.* columns.
    includeMemberOrgs: false,
  })

  if (countOnly) {
    const result = await qx.selectOne(countQuery, params)
    const count = parseInt(result.count, 10)

    await cache.setCount(countCacheKey, count, 21600)

    return {
      rows: [],
      count,
      limit,
      offset,
    }
  }

  // Prepare fields for main query
  const preparedFields = fields
    .map((f) => {
      const mappedField = QUERY_FILTER_COLUMN_MAP.get(f)
      if (!mappedField) {
        throw new Error400('en', `Invalid field: ${f}`)
      }
      return { alias: f, ...mappedField }
    })
    .filter((mappedField) => mappedField.queryable !== false)
    // Exclude fields from SELECT if their source table isn't joined:
    // - skip msa.* when aggregates aren't included (no join with memberSegmentsAgg)
    // - skip mo.* when member organizations aren't included (no join with member_orgs)
    .filter((mappedField) => {
      if (!withAggregates && mappedField.name.includes('msa.')) return false
      if (!include.memberOrganizations && mappedField.name.includes('mo.')) return false
      return true
    })
    .map((mappedField) => `${mappedField.name} AS "${mappedField.alias}"`)
    .join(',\n')

  const mainQuery = buildQuery({
    fields: preparedFields,
    withAggregates,
    includeMemberOrgs: include.memberOrganizations,
    searchConfig,
    filterString,
    orderBy,
    limit,
    offset,
  })

  const [rows, countResult] = await Promise.all([
    qx.select(mainQuery, params),
    qx.selectOne(countQuery, params),
  ])

  const count = parseInt(countResult.count, 10)
  const memberIds = rows.map((org) => org.id)

  if (memberIds.length === 0) {
    return { rows: [], count, limit, offset }
  }

  const [memberOrganizations, identities, memberSegments, maintainerRoles] = await Promise.all([
    include.memberOrganizations ? fetchManyMemberOrgs(qx, memberIds) : Promise.resolve([]),
    include.identities ? fetchManyMemberIdentities(qx, memberIds) : Promise.resolve([]),
    include.segments ? fetchManyMemberSegments(qx, memberIds) : Promise.resolve([]),
    include.maintainers ? findMaintainerRoles(qx, memberIds) : Promise.resolve([]),
  ])

  const [orgExtra, segmentsInfo, maintainerSegmentsInfo] = await Promise.all([
    include.memberOrganizations
      ? fetchOrganizationData(qx, memberOrganizations)
      : Promise.resolve({ orgs: [], lfx: [] }),
    include.segments ? fetchSegmentData(qx, memberSegments) : Promise.resolve([]),
    include.maintainers && maintainerRoles.length > 0
      ? fetchManySegments(qx, uniq(maintainerRoles.map((m) => m.segmentId)))
      : Promise.resolve([]),
  ])

  if (include.memberOrganizations) {
    const { orgs = [], lfx = [] } = orgExtra

    for (const member of rows) {
      member.organizations = []

      const memberOrgs =
        memberOrganizations.find((o) => o.memberId === member.id)?.organizations || []

      const activeOrgs = memberOrgs.filter((org) => !org.dateEnd)

      const sortedActiveOrgs = sortActiveOrganizations(activeOrgs, orgs)

      const activeOrg = sortedActiveOrgs[0]

      if (activeOrg) {
        const orgInfo = orgs.find((odn) => odn.id === activeOrg.organizationId)

        if (orgInfo) {
          const lfxMembership = lfx.find((m) => m.organizationId === activeOrg.organizationId)
          member.organizations = [
            {
              id: activeOrg.organizationId,
              displayName: orgInfo.displayName || '',
              logo: orgInfo.logo || '',
              lfxMembership: !!lfxMembership,
            },
          ]
        }
      }
    }
  }

  if (include.segments) {
    const segments = segmentsInfo || []

    rows.forEach((member) => {
      member.segments = (memberSegments.find((i) => i.memberId === member.id)?.segments || [])
        .map((segment) => {
          const segmentInfo = segments.find((s) => s.id === segment.segmentId)

          if (include.onlySubProjects && segmentInfo?.type !== SegmentType.SUB_PROJECT) {
            return null
          }

          return {
            id: segment.segmentId,
            name: segmentInfo?.name,
            activityCount: segment.activityCount,
          }
        })
        .filter(Boolean)
    })
  }

  if (include.maintainers) {
    const groupedMaintainers = groupBy(maintainerRoles, (m) => m.memberId)
    rows.forEach((member) => {
      member.maintainerRoles = (groupedMaintainers.get(member.id) || []).map((role) => {
        const segmentInfo = maintainerSegmentsInfo.find((s) => s.id === role.segmentId)
        return {
          ...role,
          segmentName: segmentInfo?.name,
        }
      })
    })
  }

  if (include.identities) {
    rows.forEach((member) => {
      const memberIdentities = identities.find((i) => i.memberId === member.id)?.identities || []

      member.identities = memberIdentities.map((identity) => ({
        type: identity.type,
        value: identity.value,
        platform: identity.platform,
        verified: identity.verified,
      }))
    })
  }

  for (const member of rows) {
    if (member.attributes) {
      // Always include default attributes for optimization
      const { isBot, jobTitle, avatarUrl, isTeamMember } = member.attributes

      const defaultAttributes = {
        ...(isBot !== undefined && { isBot }),
        ...(jobTitle !== undefined && { jobTitle }),
        ...(avatarUrl !== undefined && { avatarUrl }),
        ...(isTeamMember !== undefined && { isTeamMember }),
      }

      if (includeAllAttributes) {
        // When includeAllAttributes is true, add additional attributes to prevent data loss during updates
        const { bio, url, company, location, isHireable, websiteUrl } = member.attributes

        member.attributes = {
          ...defaultAttributes,
          ...(bio !== undefined && { bio }),
          ...(url !== undefined && { url }),
          ...(company !== undefined && { company }),
          ...(location !== undefined && { location }),
          ...(isHireable !== undefined && { isHireable }),
          ...(websiteUrl !== undefined && { websiteUrl }),
        }
      } else {
        // Default behavior: only commonly used attributes for list views
        member.attributes = defaultAttributes
      }
    }
  }

  const result = { rows, count, limit, offset }

  await Promise.all([
    cache.set(cacheKey, result, 21600),
    cache.setCount(countCacheKey, count, 21600),
  ])

  return result
}

async function refreshCacheInBackground(
  qx: QueryExecutor,
  redis: RedisClient,
  cacheKey: string,
  params: IQueryMembersAdvancedParams,
  countOnly = false,
): Promise<void> {
  const label = countOnly ? 'count cache' : 'query cache'
  const cache = new MemberQueryCache(redis)
  const acquired = await cache.tryAcquireRefreshLock(cacheKey)
  if (!acquired) {
    log.debug(
      { cacheKey },
      `Members advanced ${label} refresh already in progress — skipping duplicate`,
    )
    return
  }
  try {
    log.info({ cacheKey }, `Members advanced ${label} background refresh started`)
    await executeQuery(qx, redis, cacheKey, countOnly ? { ...params, countOnly: true } : params)
    log.info({ cacheKey }, `Members advanced ${label} background refresh completed`)
  } catch (error) {
    log.warn({ cacheKey, err: error }, `Members advanced ${label} background refresh failed`)
  } finally {
    await cache.releaseRefreshLock(cacheKey)
  }
}

function refreshCountCacheInBackground(
  qx: QueryExecutor,
  redis: RedisClient,
  cacheKey: string,
  params: IQueryMembersAdvancedParams,
): Promise<void> {
  return refreshCacheInBackground(qx, redis, cacheKey, params, true)
}

export async function queryMembers<T extends MemberField>(
  qx: QueryExecutor,
  opts: QueryOptions<T>,
): Promise<QueryResult<T>[]> {
  return queryTable(qx, 'members', Object.values(MemberField), opts)
}

export async function findMemberById<T extends MemberField>(
  qx: QueryExecutor,
  memberId: string,
  fields: T[],
): Promise<QueryResult<T>> {
  return queryTableById(qx, 'members', Object.values(MemberField), memberId, fields)
}

export async function moveAffiliationsBetweenMembers(
  qx: QueryExecutor,
  fromMemberId: string,
  toMemberId: string,
): Promise<void> {
  await qx.result(
    `
      update "memberSegmentAffiliations"
      set "memberId" = $(toMemberId)
      where
        "memberId" = $(fromMemberId)
    `,
    {
      fromMemberId,
      toMemberId,
    },
  )
}

export async function updateMember(
  qx: QueryExecutor,
  id: string,
  data: MemberUpdateInput,
): Promise<MemberRow | undefined> {
  // Only allow updating columns that actually exist in the `members` table.
  // This prevents runtime SQL errors when higher-level code passes extra fields
  // (e.g. `affiliations`, `tags`, `tasks`, etc.) that are not actually columns.
  const memberColumns = new Set<string>(Object.values(MemberField))

  const dbData: Record<string, unknown> = {}
  for (const [key, value] of Object.entries(data)) {
    // we shouldn't update id
    if (key === 'id') {
      continue
    }

    if (memberColumns.has(key)) {
      dbData[key] = value
    }
  }

  const keys = Object.keys(dbData)
  if (keys.length === 0) {
    return undefined
  }

  if (typeof dbData.displayName === 'string' && dbData.displayName) {
    dbData.displayName = getProperDisplayName(dbData.displayName)
  }

  if (Array.isArray(dbData.contributions)) {
    // Stringify array for JSONB column (pg-promise treats JS arrays as text[] by default)
    dbData.contributions = JSON.stringify(dbData.contributions)
  }

  const dbInstance = getDbInstance()

  keys.push('updatedAt')
  // construct custom column set
  const dynamicColumnSet = new dbInstance.helpers.ColumnSet(keys, {
    table: {
      table: 'members',
    },
  })

  const updatedAt = new Date()

  const prepared = prepareForModification(
    {
      ...dbData,
      updatedAt,
    },
    dynamicColumnSet,
  )
  const query = dbInstance.helpers.update(prepared, dynamicColumnSet)

  const condition = formatSql('where id = $(id) and "updatedAt" < $(updatedAt)', {
    id,
    updatedAt,
  })

  return qx.selectOneOrNone(`${query} ${condition} returning *`)
}

export async function createMember(qx: QueryExecutor, data: MemberCreateInput): Promise<MemberRow> {
  const id = data.id ?? generateUUIDv1()
  const ts = new Date()
  const dbInstance = getDbInstance()
  const columnSet = new dbInstance.helpers.ColumnSet(MEMBER_INSERT_COLUMNS, {
    table: {
      table: 'members',
    },
  })

  const dbData: Record<string, unknown> = {
    ...data,
    id,
    manuallyCreated: data.manuallyCreated ?? false,
    tenantId: DEFAULT_TENANT_ID,
    createdAt: ts,
    updatedAt: ts,
  }

  if (Array.isArray(dbData.contributions)) {
    dbData.contributions = JSON.stringify(dbData.contributions)
  }

  const prepared = prepareForModification(dbData, columnSet)

  const query = dbInstance.helpers.insert(prepared, columnSet)
  return qx.selectOne(`${query} returning *`)
}
