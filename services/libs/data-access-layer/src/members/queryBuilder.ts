type OrderDirection = 'ASC' | 'DESC'

interface SearchConfig {
  cte: string
  join: string
}

interface BuildQueryArgs {
  fields: string
  withAggregates: boolean
  includeMemberOrgs: boolean
  searchConfig: SearchConfig
  filterString: string
  orderBy?: string
  orderDirection?: OrderDirection
  limit?: number
  offset?: number
}

interface BuildCountQueryArgs {
  withAggregates: boolean
  searchConfig: SearchConfig
  filterString: string
  includeMemberOrgs?: boolean
}

const ORDER_FIELD_MAP: Record<string, string> = {
  activityCount: 'msa."activityCount"',
  score: 'm."score"',
  joinedAt: 'm."joinedAt"',
  displayName: 'm."displayName"',
}

export const buildSearchCTE = (
  search: string,
): { cte: string; join: string; params: Record<string, string> } => {
  if (!search?.trim()) {
    return { cte: '', join: '', params: {} }
  }

  const searchTerm = search.toLowerCase().trim()

  return {
    cte: `
       member_search AS (
        SELECT mi."memberId"
        FROM "memberIdentities" mi
        WHERE mi."value" ILIKE $(searchPattern)
          and mi."deletedAt" is null
        UNION
        SELECT m.id AS "memberId"
        FROM members m
        WHERE LOWER(m."displayName") LIKE $(searchPattern)
      )
    `,
    join: `INNER JOIN member_search ms ON ms."memberId" = m.id`,
    params: {
      searchPattern: `%${searchTerm}%`,
    },
  }
}

export const buildMemberOrgsCTE = (includeMemberOrgs: boolean): string => {
  if (!includeMemberOrgs) return ''

  return `
    member_orgs AS (
      SELECT
        "memberId",
        ARRAY_AGG("organizationId"::TEXT) AS "organizationId"
      FROM "memberOrganizations"
      WHERE "deletedAt" IS NULL
      GROUP BY "memberId"
    )
  `
}

const parseOrderBy = (
  orderBy: string | undefined,
  fallbackDirection: OrderDirection,
): { field?: string; direction: OrderDirection } => {
  const defaultDirection: OrderDirection = fallbackDirection || 'DESC'

  if (!orderBy || !orderBy.trim()) {
    return { field: undefined, direction: defaultDirection }
  }

  const [rawField, rawDir] = orderBy.trim().split('_')
  const field = rawField?.trim() || undefined

  const dir = (rawDir || '').toUpperCase()
  const direction: OrderDirection =
    dir === 'ASC' || dir === 'DESC' ? (dir as OrderDirection) : defaultDirection

  return { field, direction }
}

const getOrderClause = (
  parsedField: string | undefined,
  direction: OrderDirection,
  withAggregates: boolean,
): string => {
  const defaultOrder = withAggregates ? 'msa."activityCount" DESC' : 'm."joinedAt" DESC'

  if (!parsedField) return defaultOrder

  const fieldExpr = ORDER_FIELD_MAP[parsedField]

  if (!fieldExpr) return defaultOrder

  if (!withAggregates && fieldExpr.includes('msa.')) {
    return defaultOrder
  }

  return `${fieldExpr} ${direction}`
}

/**
 * Analyzes the filter string to determine if it targets specific member IDs,
 * which allows for query optimization by skipping expensive sorting operations.
 *
 * @param filterString - The SQL filter condition string
 * @returns Object indicating if members are pinned to specific IDs and if the list is small
 */
const analyzeMemberIdTargeting = (
  filterString: string,
): {
  isTargetingSpecificMembers: boolean
  hasSmallMemberSet: boolean
} => {
  if (!filterString?.trim()) {
    return { isTargetingSpecificMembers: false, hasSmallMemberSet: false }
  }

  // Check for single member ID equality: m.id = '...' or m.id = $(param) or m.id = :param
  const singleIdPattern = /\bm\.id\s*=\s*(?:'[^']+'|\$\([^)]+\)|:[a-zA-Z_]\w*|\?)/i
  if (singleIdPattern.test(filterString)) {
    return { isTargetingSpecificMembers: true, hasSmallMemberSet: true }
  }

  // Check for member ID list: m.id IN (...)
  const idListPattern = /\bm\.id\s+IN\s*\(([^)]+)\)/i
  const listMatch = idListPattern.exec(filterString)

  if (listMatch?.length > 1) {
    // Count items in the IN clause by splitting on commas
    const itemsInList = listMatch[1]
      .split(',')
      .map((item) => item.trim())
      .filter((item) => item.length > 0)

    // Consider lists with <= 100 items as "small" for optimization purposes
    const isSmallList = itemsInList.length <= 100

    return { isTargetingSpecificMembers: true, hasSmallMemberSet: isSmallList }
  }

  // No specific member targeting found
  return { isTargetingSpecificMembers: false, hasSmallMemberSet: false }
}

/**
 * Checks if the filter string contains references to members table fields
 * excluding m.id (which is handled separately for optimization purposes).
 *
 * @param filterString - The SQL filter condition string
 * @returns true if filter uses members table fields other than m.id
 */
const hasNonIdMemberFieldReferences = (filterString: string): boolean => {
  if (!filterString.includes('m.')) {
    return false
  }

  // Remove all m.id references from the filter string
  const filterWithoutMemberIds = filterString.replace(/\bm\.id\b/g, '')

  // Check if there are still any m.* field references remaining
  return /\bm\.\w+/.test(filterWithoutMemberIds)
}

/**
 * Determines if we can use the activityCount optimization strategy.
 * This optimization creates a CTE with top members by activity count,
 * which is much faster than sorting the entire dataset.
 *
 * @param withAggregates - Whether aggregates are available
 * @param sortField - The field being sorted by (undefined means default activityCount)
 * @param hasEnrichmentFilters - Whether filter references me.* fields
 * @param hasOrganizationFilters - Whether filter references mo.* fields
 * @returns true if activityCount optimization can be used
 */
const canUseActivityCountOptimization = ({
  filterHasMe,
  filterHasMo,
  includeMemberOrgs,
  sortField,
  withAggregates,
}: {
  filterHasMe: boolean
  filterHasMo: boolean
  sortField: string | undefined
  withAggregates: boolean
  includeMemberOrgs: boolean
}): boolean => {
  // Need aggregates to access activityCount
  if (!withAggregates) return false

  // Only works when sorting by activityCount (or using default sort)
  if (sortField && sortField !== 'activityCount') return false

  // Cannot use if filter requires expensive joins (me.*, mo.*)
  if (filterHasMe || filterHasMo) return false

  if (includeMemberOrgs) return false

  return true
}

/**
 * Builds optimized query for when we're targeting specific member IDs.
 * This path starts from memberSegmentsAgg and avoids expensive sorting operations.
 *
 * @param fields - The SELECT fields to return
 * @param filterString - The WHERE clause filter
 * @param orderClause - The ORDER BY clause
 * @param searchConfig - Search CTE configuration
 * @param needsMemberOrgs - Whether to include member organizations
 * @param limit - Query limit
 * @param offset - Query offset
 * @returns The optimized SQL query string
 */
const buildDirectIdPathQuery = ({
  fields,
  filterString,
  orderClause,
  searchConfig,
  needsMemberOrgs,
  limit,
  offset,
}: {
  fields: string
  filterString: string
  orderClause: string
  searchConfig: SearchConfig
  needsMemberOrgs: boolean
  limit: number
  offset: number
}): string => {
  // Build CTEs array
  const ctes: string[] = []
  if (needsMemberOrgs) ctes.push(buildMemberOrgsCTE(true).trim())
  if (searchConfig.cte) ctes.push(searchConfig.cte.trim())

  const withClause = ctes.length ? `WITH ${ctes.join(',\n')}` : ''

  // Build JOIN clauses
  const memberOrgsJoin = needsMemberOrgs ? `LEFT JOIN member_orgs mo ON mo."memberId" = m.id` : ''
  const searchJoin = searchConfig.join || ''

  return `
    ${withClause}
    SELECT ${fields}
    FROM "memberSegmentsAgg" msa
    JOIN members m
      ON m.id = msa."memberId"
    ${memberOrgsJoin}
    ${searchJoin}
    LEFT JOIN "memberEnrichments" me
      ON me."memberId" = m.id
    WHERE
      msa."segmentId" = $(segmentId)
      AND (${filterString})
    ORDER BY ${orderClause} NULLS LAST
    LIMIT ${limit}
    OFFSET ${offset}
  `.trim()
}

/**
 * Builds optimized query using top-N activity count strategy.
 * This creates a CTE with the most active members first, then joins back for full data.
 * Much faster than sorting the entire member dataset.
 *
 * @param fields - The SELECT fields to return
 * @param filterString - The WHERE clause filter
 * @param searchConfig - Search CTE configuration
 * @param direction - Sort direction for activityCount
 * @param hasNonIdMemberFields - Whether filter uses m.* fields (requires oversampling)
 * @param limit - Query limit
 * @param offset - Query offset
 * @returns The optimized SQL query string
 */
const buildActivityCountOptimizedQuery = ({
  fields,
  filterString,
  searchConfig,
  direction,
  hasNonIdMemberFields,
  limit,
  offset,
}: {
  fields: string
  filterString: string
  searchConfig: SearchConfig
  direction: OrderDirection
  hasNonIdMemberFields: boolean
  limit: number
  offset: number
}): string => {
  const ctes: string[] = []
  if (searchConfig.cte) ctes.push(searchConfig.cte.trim())

  const searchJoinForTopMembers = searchConfig.cte
    ? `\n        INNER JOIN member_search ms ON ms."memberId" = msa."memberId"`
    : ''

  const baseNeeded = limit + offset
  const oversampleMultiplier = hasNonIdMemberFields ? 10 : 1 // 10x oversampling for m.* filters
  const totalNeeded = Math.min(baseNeeded * oversampleMultiplier, 50000) // Cap at 50k

  ctes.push(
    `
    top_members AS (
      SELECT
        msa."memberId",
        msa."activityCount"
      FROM "memberSegmentsAgg" msa
      INNER JOIN members m ON m.id = msa."memberId"
      ${searchJoinForTopMembers}
      WHERE
        msa."segmentId" = $(segmentId)
        AND (${filterString})
      ORDER BY
        msa."activityCount" ${direction} NULLS LAST
      LIMIT ${totalNeeded}
    )
  `.trim(),
  )

  const withClause = `WITH ${ctes.join(',\n')}`

  // Outer query is much simpler now - no more filtering needed
  return `
    ${withClause}
    SELECT ${fields}
    FROM top_members tm
    JOIN members m
      ON m.id = tm."memberId"
    INNER JOIN "memberSegmentsAgg" msa
      ON msa."memberId" = m.id
     AND msa."segmentId" = $(segmentId)
    LEFT JOIN "memberEnrichments" me
      ON me."memberId" = m.id
    ORDER BY
      msa."activityCount" ${direction} NULLS LAST
    LIMIT ${limit}
    OFFSET ${offset}
  `.trim()
}

export const buildQuery = ({
  fields,
  withAggregates,
  includeMemberOrgs,
  searchConfig,
  filterString,
  orderBy,
  orderDirection,
  limit = 20,
  offset = 0,
}: BuildQueryArgs): string => {
  const { field: sortField, direction } = parseOrderBy(orderBy, orderDirection)

  // Detect alias usage in filters (to decide joins/CTEs needed outside)
  const filterHasMo = filterString.includes('mo.')
  const filterHasMe = filterString.includes('me.')
  const filterHasNonIdMemberFields = hasNonIdMemberFieldReferences(filterString)

  const needsMemberOrgs = includeMemberOrgs || filterHasMo

  // If filters pin m.id to a single value or a small IN-list, skip top-N entirely.
  const { isTargetingSpecificMembers, hasSmallMemberSet } = analyzeMemberIdTargeting(filterString)
  const useDirectIdPath = withAggregates && isTargetingSpecificMembers && hasSmallMemberSet

  // Default sort clause for fallback/outer queries
  const orderClause = getOrderClause(sortField, direction, withAggregates)

  if (useDirectIdPath) {
    return buildDirectIdPathQuery({
      fields,
      filterString,
      orderClause,
      searchConfig,
      needsMemberOrgs,
      limit,
      offset,
    })
  }

  const useActivityCountOptimized = canUseActivityCountOptimization({
    filterHasMe,
    filterHasMo,
    includeMemberOrgs,
    sortField,
    withAggregates,
  })

  if (useActivityCountOptimized) {
    return buildActivityCountOptimizedQuery({
      fields,
      filterString,
      searchConfig,
      direction,
      hasNonIdMemberFields: filterHasNonIdMemberFields,
      limit,
      offset,
    })
  }

  // Fallback path (other sorts / non-aggregate / filtered queries)
  const baseCtes = [needsMemberOrgs ? buildMemberOrgsCTE(true) : '', searchConfig.cte].filter(
    Boolean,
  )

  const joins = [
    withAggregates
      ? `INNER JOIN "memberSegmentsAgg" msa ON msa."memberId" = m.id AND msa."segmentId" = $(segmentId)`
      : '',
    needsMemberOrgs ? `LEFT JOIN member_orgs mo ON mo."memberId" = m.id` : '',
    `LEFT JOIN "memberEnrichments" me ON me."memberId" = m.id`,
    searchConfig.join,
  ].filter(Boolean)

  return `
    ${baseCtes.length ? `WITH ${baseCtes.join(',\n')}` : ''}
    SELECT ${fields}
    FROM members m
    ${joins.join('\n')}
    WHERE (${filterString})
    ORDER BY ${orderClause} NULLS LAST
    LIMIT ${limit}
    OFFSET ${offset}
  `.trim()
}

export const buildCountQuery = ({
  withAggregates,
  searchConfig,
  filterString,
  includeMemberOrgs = false,
}: BuildCountQueryArgs): string => {
  const filterHasMo = filterString.includes('mo.')
  const filterHasMe = filterString.includes('me.')
  const needsMemberOrgs = includeMemberOrgs || filterHasMo

  const ctes = [needsMemberOrgs ? buildMemberOrgsCTE(true) : '', searchConfig.cte].filter(Boolean)

  const joins = [
    withAggregates
      ? `INNER JOIN "memberSegmentsAgg" msa ON msa."memberId" = m.id AND msa."segmentId" = $(segmentId)`
      : '',
    needsMemberOrgs ? `LEFT JOIN member_orgs mo ON mo."memberId" = m.id` : '',
    filterHasMe ? `LEFT JOIN "memberEnrichments" me ON me."memberId" = m.id` : '',
    searchConfig.join,
  ].filter(Boolean)

  return `
    ${ctes.length > 0 ? `WITH ${ctes.join(',\n')}` : ''}
    SELECT COUNT(DISTINCT m.id) AS count
    FROM members m
    ${joins.join('\n')}
    WHERE (${filterString})
  `.trim()
}
