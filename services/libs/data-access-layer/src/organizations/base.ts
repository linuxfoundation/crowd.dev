import {
  DEFAULT_TENANT_ID,
  UnrepeatableError,
  generateOrganizationNameVariants,
  generateUUIDv1,
  normalizeHostname,
} from '@crowd/common'
import { getServiceChildLogger, logExecutionTimeV2 } from '@crowd/logging'
import {
  IMemberOrganization,
  IOrganization,
  IOrganizationIdSource,
  IOrganizationIdentity,
  IQueryTimeseriesParams,
  ITimeseriesDatapoint,
  OrganizationDbInsert,
  OrganizationDbRow,
  OrganizationIdentityType,
} from '@crowd/types'

import { QueryExecutor } from '../queryExecutor'
import { findManyLfSegmentsByNames } from '../segments'
import { QueryOptions, QueryResult, prepareBulkInsert, queryTable, queryTableById } from '../utils'
import { prepareSelectColumns } from '../utils'
import {
  hasAllOrgAttributes,
  isOrgAttributeDefault,
  markOrgAttributeDefault,
  upsertOrgAttributes,
} from './attributes'
import {
  hasAllOrgIdentities,
  insertOrganizationIdentities,
  upsertOrgIdentities,
} from './identities'
import {
  IDbOrgAttribute,
  IDbOrganization,
  IDbOrganizationInput,
  IFindOrCreateOrganizationResult,
} from './types'
import { prepareOrganizationData } from './utils'

const log = getServiceChildLogger('data-access-layer/organizations')

const ORG_SELECT_COLUMNS = [
  'id',
  'description',
  'displayName',
  'logo',
  'tags',
  'employees',
  'revenueRange',
  'importHash',
  'location',
  'country',
  'isTeamOrganization',
  'isAffiliationBlocked',
  'type',
  'size',
  'headline',
  'industry',
  'founded',
  'employeeChurnRate',
  'employeeGrowthRate',
  'manuallyCreated',
]

export async function findOrgIdByUniqueDisplayName(
  qx: QueryExecutor,
  displayName: string,
): Promise<string | null> {
  const rows: { id: string }[] = await qx.select(
    `
      SELECT id
      FROM organizations
      WHERE TRIM(BOTH FROM lower("displayName")) = TRIM(BOTH FROM lower($(displayName)))
        AND "deletedAt" IS NULL
      LIMIT 2
    `,
    { displayName },
  )

  return rows.length === 1 ? rows[0].id : null
}

export async function findOrgBySourceId(
  qx: QueryExecutor,
  segmentId: string,
  platform: string,
  sourceId: string,
): Promise<IDbOrganization | null> {
  const result = await qx.selectOneOrNone(
    `
    with
        "organizationsWithSourceIdAndSegment" as (
            select oi."organizationId"
            from "organizationIdentities" oi
            join "organizationSegments" os on oi."organizationId" = os."organizationId"
            where
                  oi.platform = $(platform)
                  and oi."sourceId" = $(sourceId)
                  and os."segmentId" =  $(segmentId)
            order by oi."updatedAt" desc
            limit 1
        )
    select ${prepareSelectColumns(ORG_SELECT_COLUMNS, 'o')}
    from organizations o
    where o.id in (select distinct "organizationId" from "organizationsWithSourceIdAndSegment");`,
    { sourceId, segmentId, platform },
  )

  return result
}

export async function findOrgsByIds(
  qx: QueryExecutor,
  organizationIds: string[],
): Promise<IDbOrganization[]> {
  if (!organizationIds.length) return []
  const results = await qx.select(
    `
    select ${prepareSelectColumns(ORG_SELECT_COLUMNS, 'o')}
    from organizations o
    where o.id = ANY($(organizationIds)::uuid[])
    `,
    { organizationIds },
  )
  return results
}

export async function findExistingOrganizationIds(
  qx: QueryExecutor,
  organizationIds: string[],
): Promise<string[]> {
  if (!organizationIds.length) return []
  const rows: { id: string }[] = await qx.select(
    `
    select o.id
    from organizations o
    where o.id = ANY($(organizationIds)::uuid[]) and o."deletedAt" is null
    `,
    { organizationIds },
  )
  return rows.map((row) => row.id)
}

export async function findManyOrganizationsByNames(
  qx: QueryExecutor,
  names: string[],
): Promise<IDbOrganization[]> {
  const normalized = names
    .map((name) => name?.trim().toLowerCase())
    .filter((name): name is string => Boolean(name))

  if (normalized.length === 0) {
    return []
  }

  return qx.select(
    `
      select ${prepareSelectColumns(ORG_SELECT_COLUMNS, 'o')}
      from organizations o
      where o."deletedAt" is null
        and trim(lower(o."displayName")) in ($(names:csv))
    `,
    { names: normalized },
  )
}

export async function findOrgByVerifiedDomain(
  qx: QueryExecutor,
  identity: IOrganizationIdentity,
): Promise<IDbOrganization | null> {
  if (identity.type !== OrganizationIdentityType.PRIMARY_DOMAIN) {
    throw new Error('Invalid identity type')
  }
  const result = await qx.selectOneOrNone(
    `
    with "organizationsWithIdentity" as (
              select oi."organizationId"
              from "organizationIdentities" oi
              where
                    lower(oi.value) = lower($(value))
                    and oi.type = $(type)
                    and oi.verified = true
          )
          select  ${prepareSelectColumns(ORG_SELECT_COLUMNS, 'o')}
          from organizations o
          where o.id in (select distinct "organizationId" from "organizationsWithIdentity")
          limit 1;
    `,
    {
      value: identity.value,
      platform: identity.platform,
      type: identity.type,
    },
  )

  return result
}

export async function findOrgByVerifiedIdentity(
  qx: QueryExecutor,
  identity: IOrganizationIdentity,
): Promise<IDbOrganization | null> {
  const result = await qx.selectOneOrNone(
    `
    with "organizationsWithIdentity" as (
              select oi."organizationId"
              from "organizationIdentities" oi
              where
                    oi.platform = $(platform)
                    and lower(oi.value) = lower($(value))
                    and oi.type = $(type)
                    and oi.verified = true
          )
          select  ${prepareSelectColumns(ORG_SELECT_COLUMNS, 'o')}
          from organizations o
          where o.id in (select distinct "organizationId" from "organizationsWithIdentity")
          limit 1;
    `,
    {
      value: identity.value,
      platform: identity.platform,
      type: identity.type,
    },
  )

  return result
}

export async function markOrganizationEnriched(
  qe: QueryExecutor,
  organizationId: string,
): Promise<void> {
  await qe.selectNone(
    `
    update organizations
    set "lastEnrichedAt" = now()
    where id = $(organizationId)
    `,
    {
      organizationId,
    },
  )
}

export async function addOrgsToSegments(
  qe: QueryExecutor,
  segmentIds: string[],
  orgIds: string[],
): Promise<void> {
  if (orgIds.length === 0 || segmentIds.length === 0) {
    return
  }

  const parameters: Record<string, unknown> = {
    tenantId: DEFAULT_TENANT_ID,
  }

  const valueStrings = []
  for (let i = 0; i < orgIds.length; i++) {
    const orgParam = `orgId_${i}`
    const orgId = orgIds[i]
    parameters[orgParam] = orgId

    for (let j = 0; j < segmentIds.length; j++) {
      const segmentParam = `segmentId_${i}_${j}`
      parameters[segmentParam] = segmentIds[j]
      valueStrings.push(`($(tenantId), $(${segmentParam}), $(orgId_${i}), now())`)
    }
  }

  const valueString = valueStrings.join(',')

  const query = `
  insert into "organizationSegments"("tenantId", "segmentId", "organizationId", "createdAt")
  values ${valueString}
  on conflict do nothing;
  `

  await qe.selectNone(query, parameters)
}

export async function addOrgsToMember(
  qe: QueryExecutor,
  memberId: string,
  orgs: IOrganizationIdSource[],
): Promise<
  {
    memberOrganizationId: string
    organizationId: string
  }[]
> {
  if (orgs.length === 0) {
    return []
  }

  const parameters: Record<string, unknown> = {
    memberId,
  }

  const valueStrings = []
  for (let i = 0; i < orgs.length; i++) {
    const org = orgs[i]
    parameters[`orgId_${i}`] = org.id
    parameters[`source_${i}`] = org.source
    valueStrings.push(`($(orgId_${i}), $(memberId), now(), now(), $(source_${i}))`)
  }

  const valueString = valueStrings.join(',')

  const query = `
  insert into "memberOrganizations"("organizationId", "memberId", "createdAt", "updatedAt", "source")
  values ${valueString}
  on conflict do nothing
  returning id, "organizationId";
  `

  const rows = await qe.select(query, parameters)

  return rows.map((r) => ({
    memberOrganizationId: r.id,
    organizationId: r.organizationId,
  }))
}

export async function findMemberOrganizations(
  qe: QueryExecutor,
  memberId: string,
  organizationId: string,
): Promise<IMemberOrganization[]> {
  return await qe.select(
    `
    select *
    from "memberOrganizations"
    where "memberId" = $(memberId) and "organizationId" = $(organizationId)
    `,
    {
      memberId,
      organizationId,
    },
  )
}

export async function insertOrganization(
  qe: QueryExecutor,
  data: IDbOrganizationInput,
): Promise<string> {
  const columns = Object.keys(data)

  if (columns.length === 0) {
    throw new Error('No data to insert')
  }

  const id = generateUUIDv1()
  const now = new Date()

  columns.push('id')
  columns.push('tenantId')
  columns.push('createdAt')
  columns.push('updatedAt')

  const query = `
    insert into organizations(${columns.map((c) => `"${c}"`).join(', ')})
    values(${columns.map((c) => `$(${c})`).join(', ')})
  `

  const rowCount = await qe.result(query, {
    ...data,
    id,
    tenantId: DEFAULT_TENANT_ID,
    createdAt: now,
    updatedAt: now,
  })

  if (rowCount !== 1) {
    throw new Error('Failed to insert organization')
  }

  return id
}

export async function insertOrganizations(
  qx: QueryExecutor,
  organizations: OrganizationDbInsert[],
  failOnConflict: boolean,
  returnRows: true,
): Promise<OrganizationDbRow[]>
export async function insertOrganizations(
  qx: QueryExecutor,
  organizations: OrganizationDbInsert[],
  failOnConflict?: boolean,
  returnRows?: false,
): Promise<number>
export async function insertOrganizations(
  qx: QueryExecutor,
  organizations: OrganizationDbInsert[],
  failOnConflict = false,
  returnRows = false,
): Promise<OrganizationDbRow[] | number> {
  if (organizations.length === 0) {
    return returnRows ? [] : 0
  }

  const ts = new Date()

  const query = prepareBulkInsert(
    'organizations',
    [
      'id',
      'displayName',
      'description',
      'logo',
      'tags',
      'employees',
      'revenueRange',
      'importHash',
      'location',
      'country',
      'isTeamOrganization',
      'isAffiliationBlocked',
      'type',
      'size',
      'headline',
      'industry',
      'founded',
      'employeeChurnRate',
      'employeeGrowthRate',
      'manuallyCreated',
      'createdById',
      'updatedById',
      'tenantId',
      'createdAt',
      'updatedAt',
    ],
    organizations.map((o) => ({
      ...o,
      id: o.id ?? generateUUIDv1(),
      tenantId: DEFAULT_TENANT_ID,
      createdAt: ts,
      updatedAt: ts,
      // NOT NULL DEFAULT false — must set while column is in INSERT list
      isTeamOrganization: o.isTeamOrganization ?? false,
      isAffiliationBlocked: o.isAffiliationBlocked ?? false,
      manuallyCreated: o.manuallyCreated ?? false,
    })),
    failOnConflict ? undefined : 'DO NOTHING',
    returnRows,
  )

  if (returnRows) {
    return qx.select(query)
  }

  return qx.result(query)
}

export async function updateOrganization(
  qe: QueryExecutor,
  organizationId: string,
  data: Partial<IDbOrganizationInput>,
  throttleUpdatedAt = false,
): Promise<string | null> {
  const columns = Object.keys(data)
  if (columns.length === 0) {
    return null
  }

  const updatedAt = new Date()
  columns.push('updatedAt')

  const updatedAtExpr = throttleUpdatedAt
    ? `CASE WHEN "updatedAt" < now() - interval '30 minutes' THEN now() ELSE "updatedAt" END`
    : `$(updatedAt)`

  const query = `
    update organizations set
      ${columns.map((c) => `"${c}" = ${c === 'updatedAt' ? updatedAtExpr : `$(${c})`}`).join(',\n')}
    where id = $(organizationId)
    returning id;
  `

  const result = await qe.selectOneOrNone(query, {
    ...data,
    organizationId,
    updatedAt,
  })

  if (!result) {
    return null
  }

  return result.id
}

export async function getTimeseriesOfNewOrganizations(
  qx: QueryExecutor,
  params: IQueryTimeseriesParams,
): Promise<ITimeseriesDatapoint[]> {
  const query = `
    SELECT
      COUNT(DISTINCT o.id) AS count,
      TO_CHAR(osa."joinedAt", 'YYYY-MM-DD') AS "date"
    FROM organizations AS o
    JOIN "organizationSegmentsAgg" osa ON osa."organizationId" = o.id
    WHERE osa."joinedAt" >= $(startDate)
      AND osa."joinedAt" < $(endDate)
      ${params.segmentIds ? 'AND osa."segmentId" IN ($(segmentIds:csv))' : 'AND osa."segmentId" IS NULL'}
      ${params.platform ? 'AND $(platform) = ANY(osa."activeOn")' : ''}
    GROUP BY 2
    ORDER BY 2
  `

  return qx.select(query, { ...params })
}

export async function getTimeseriesOfActiveOrganizations(
  qx: QueryExecutor,
  params: IQueryTimeseriesParams,
): Promise<ITimeseriesDatapoint[]> {
  const query = `
    SELECT
      COUNT(DISTINCT "organizationId") AS count,
      DATE_TRUNC('day', timestamp) as date
    FROM "activityRelations"
    WHERE "organizationId" IS NOT NULL
      ${params.segmentIds ? 'AND "segmentId" IN ($(segmentIds:csv))' : ''}
      AND timestamp >= $(startDate)
      AND timestamp < $(endDate)
      ${params.platform ? 'AND "platform" = $(platform)' : ''}
    GROUP BY 2
    ORDER BY 2
  `

  return qx.select(query, { ...params })
}

async function findOrgForUpdate(qe: QueryExecutor, id: string): Promise<IDbOrganization | null> {
  return qe.selectOneOrNone(
    `select ${prepareSelectColumns(ORG_SELECT_COLUMNS, 'o')} from organizations o where o.id = $(id)`,
    { id },
  )
}

interface IStoredOrg {
  attributes: IDbOrgAttribute[]
  identities: IOrganizationIdentity[]
}

async function findOrgAttributesAndIdentities(qe: QueryExecutor, id: string): Promise<IStoredOrg> {
  return qe.selectOne(
    `
      select
        coalesce((select jsonb_agg(oa) from "orgAttributes" oa where oa."organizationId" = $(id)), '[]') as attributes,
        coalesce((select jsonb_agg(oi) from "organizationIdentities" oi where oi."organizationId" = $(id)), '[]') as identities
    `,
    { id },
  )
}

function planOrgUpdate(
  data: IOrganization,
  source: string,
  existing: IDbOrganization,
  stored: IStoredOrg,
) {
  // prepareOrganizationData rewrites the default flags of the attributes it gets, so it gets copies
  const processed = prepareOrganizationData(
    data,
    source,
    existing,
    stored.attributes.map((a) => ({ ...a })),
  )
  const updateOrganization = Object.keys(processed.organization).length > 0
  const upsertIdentities = !hasAllOrgIdentities(stored.identities, data.identities)
  const upsertAttributes = !hasAllOrgAttributes(stored.attributes, processed.attributes)
  const markDefaults = processed.attributes.filter(
    (a) => a.default && !isOrgAttributeDefault(stored.attributes, a),
  )
  return {
    processed,
    updateOrganization,
    upsertIdentities,
    upsertAttributes,
    markDefaults,
    changesNothing:
      !updateOrganization && !upsertIdentities && !upsertAttributes && markDefaults.length === 0,
  }
}

// lockOrganization: a caller that runs this concurrently without transactions passes a lock, so
// calls that reach the same organization through different identities update it one at a time
export async function findOrCreateOrganization(
  qe: QueryExecutor,
  source: string,
  data: IOrganization,
  integrationId?: string,
  throttleUpdatedAt = false,
  lockOrganization?: (organizationId: string) => Promise<() => void>,
): Promise<IFindOrCreateOrganizationResult | undefined> {
  data.identities = data.identities ?? []
  let verifiedIdentities = data.identities.filter((i) => i.verified)

  if (verifiedIdentities.length === 0 && !data.displayName) {
    const message = `Missing organization identity or displayName while creating/updating organization!`
    log.error(data, message)
    throw new UnrepeatableError(message)
  }

  try {
    // Normalize the website identities
    for (const identity of data.identities.filter((i) =>
      [
        OrganizationIdentityType.PRIMARY_DOMAIN,
        OrganizationIdentityType.ALTERNATIVE_DOMAIN,
      ].includes(i.type),
    )) {
      identity.value = normalizeHostname(identity.value, false)
    }

    data.identities = data.identities.filter((i) => i.value !== undefined)

    // Re-derive after normalization may have set domain identity values to undefined
    verifiedIdentities = data.identities.filter((i) => i.verified)

    if (verifiedIdentities.length === 0 && !data.displayName) {
      log.debug(
        'Organization has no valid verified identities after domain normalization and no displayName, skipping.',
      )
      return undefined
    }

    let existing
    // find existing org by sent verified identities
    for (const identity of verifiedIdentities) {
      existing = await logExecutionTimeV2(
        async () => findOrgByVerifiedIdentity(qe, identity),
        log,
        'organizationService -> findOrCreateOrganization -> findOrgByVerifiedIdentity',
      )

      if (!existing && identity.type === OrganizationIdentityType.PRIMARY_DOMAIN) {
        // if primary domain isn't found in the incoming platform, check if the domain exists in any platform
        existing = await logExecutionTimeV2(
          async () => findOrgByVerifiedDomain(qe, identity),
          log,
          'organizationService -> findOrCreateOrganization -> findOrgByVerifiedDomain',
        )
      }
      if (existing) {
        break
      }
    }

    if (!existing) {
      const organizations = await logExecutionTimeV2(
        async () => findManyOrganizationsByNames(qe, [data.displayName]),
        log,
        'organizationService -> findOrCreateOrganization -> findManyOrganizationsByNames',
      )

      if (organizations.length > 0) {
        existing = organizations[0]
      }
    }

    let id: string

    if (!existing && verifiedIdentities.length === 0) {
      log.debug(
        'Organization does not have any verified identities and was not found by name so we will not create it.',
      )
      return undefined
    }

    if (existing) {
      log.trace(`Found existing organization, organization will be updated!`)

      let stored = await logExecutionTimeV2(
        async () => findOrgAttributesAndIdentities(qe, existing.id),
        log,
        'organizationService -> findOrCreateOrganization -> findOrgAttributesAndIdentities',
      )
      let plan = planOrgUpdate(data, source, existing, stored)
      if (plan.changesNothing) {
        return { id: existing.id, created: false }
      }

      const release = await lockOrganization?.(existing.id)
      try {
        if (release) {
          // read it again: the call that held the lock may have changed it
          existing = (await findOrgForUpdate(qe, existing.id)) ?? existing
          stored = await findOrgAttributesAndIdentities(qe, existing.id)
          plan = planOrgUpdate(data, source, existing, stored)
        }

        const { processed } = plan
        log.trace({ updateData: processed.organization }, `Updating organization!`)

        if (plan.updateOrganization) {
          log.info({ orgId: existing.id }, `Updating organization!`)
          await logExecutionTimeV2(
            async () =>
              updateOrganization(qe, existing.id, processed.organization, throttleUpdatedAt),
            log,
            'organizationService -> findOrCreateOrganization -> updateOrganization',
          )
        }
        if (plan.upsertIdentities) {
          await logExecutionTimeV2(
            async () =>
              upsertOrgIdentities(
                qe,
                existing.id,
                data.identities.map((i) => ({ ...i, source })),
                integrationId,
              ),
            log,
            'organizationService -> findOrCreateOrganization -> upsertOrgIdentities',
          )
        }
        if (plan.upsertAttributes) {
          await logExecutionTimeV2(
            async () => upsertOrgAttributes(qe, existing.id, processed.attributes),
            log,
            'organizationService -> findOrCreateOrganization -> upsertOrgAttributes',
          )
        }
        for (const attr of plan.markDefaults) {
          await logExecutionTimeV2(
            async () => markOrgAttributeDefault(qe, existing.id, attr),
            log,
            'organizationService -> findOrCreateOrganization -> markOrgAttributeDefault',
          )
        }

        id = existing.id
      } finally {
        release?.()
      }
    } else {
      log.trace(`Organization wasn't found via website or identities.`)
      const displayName = data.displayName ? data.displayName : verifiedIdentities[0].value

      const payload: Partial<IDbOrganizationInput> = {
        displayName,
        description: data.description,
        logo: data.logo,
        tags: data.tags,
        employees: data.employees,
        location: data.location,
        country: data.country,
        type: data.type,
        size: data.size,
        headline: data.headline,
        industry: data.industry,
        founded: data.founded,
      }

      // Block organization affiliation if a segment (project, subproject, or project group)
      // has the same name as the organization when creating one.
      const lfSegments = await findManyLfSegmentsByNames(
        qe,
        generateOrganizationNameVariants(displayName),
      )
      if (lfSegments.length > 0) {
        payload.isAffiliationBlocked = true
      }

      log.trace({ data, payload }, `Preparing payload to create a new organization!`)

      const processed = prepareOrganizationData(payload, source)

      log.trace({ payload: processed }, `Creating new organization!`)

      // if it doesn't exists create it
      id = await logExecutionTimeV2(
        async () => insertOrganization(qe, processed.organization),
        log,
        'organizationService -> findOrCreateOrganization -> insertOrganization',
      )

      await logExecutionTimeV2(
        async () => upsertOrgAttributes(qe, id, processed.attributes),
        log,
        'organizationService -> findOrCreateOrganization -> upsertOrgAttributes',
      )
      for (const attr of processed.attributes) {
        if (attr.default) {
          await logExecutionTimeV2(
            async () => markOrgAttributeDefault(qe, id, attr),
            log,
            'organizationService -> findOrCreateOrganization -> markOrgAttributeDefault',
          )
        }
      }

      // create identities
      if (data.identities.length > 0) {
        await logExecutionTimeV2(
          () =>
            insertOrganizationIdentities(
              qe,
              data.identities.map((i) => ({
                organizationId: id,
                platform: i.platform,
                type: i.type,
                value: i.value,
                verified: i.verified,
                sourceId: i.sourceId,
                integrationId,
                source: i.source,
              })),
              false,
            ),
          log,
          'organizationService -> findOrCreateOrganization -> insertOrganizationIdentities',
        )
      }
    }

    return { id, created: !existing }
  } catch (err) {
    log.error(err, 'Error while upserting an organization!')
    throw err
  }
}

export enum OrganizationField {
  // meta
  ID = 'id',
  CREATED_AT = 'createdAt',
  UPDATED_AT = 'updatedAt',
  DELETED_AT = 'deletedAt',
  CREATED_BY_ID = 'createdById',
  UPDATED_BY_ID = 'updatedById',
  TENANT_ID = 'tenantId',

  IMPORT_HASH = 'importHash',
  LAST_ENRICHED_AT = 'lastEnrichedAt',
  MANUALLY_CREATED = 'manuallyCreated',

  DESCRIPTION = 'description',
  LOGO = 'logo',
  TAGS = 'tags',
  EMPLOYEES = 'employees',
  REVENUE_RANGE = 'revenueRange',
  LOCATION = 'location',
  COUNTRY = 'country',
  IS_TEAM_ORGANIZATION = 'isTeamOrganization',
  IS_AFFILIATION_BLOCKED = 'isAffiliationBlocked',
  TYPE = 'type',
  SIZE = 'size',
  HEADLINE = 'headline',
  INDUSTRY = 'industry',
  FOUNDED = 'founded',
  DISPLAY_NAME = 'displayName',
  EMPLOYEE_CHURN_RATE = 'employeeChurnRate',
  EMPLOYEE_GROWTH_RATE = 'employeeGrowthRate',
}

export async function queryOrgs<T extends OrganizationField>(
  qx: QueryExecutor,
  opts: QueryOptions<T>,
): Promise<QueryResult<T>[]> {
  return queryTable(qx, 'organizations', Object.values(OrganizationField), opts)
}

export async function findOrgById<T extends OrganizationField>(
  qx: QueryExecutor,
  orgId: string,
  fields: T[],
): Promise<QueryResult<T>> {
  return queryTableById(qx, 'organizations', Object.values(OrganizationField), orgId, fields)
}

export async function findNonExistingOrganizationIds(
  qx: QueryExecutor,
  ids: string[],
): Promise<string[]> {
  if (ids.length === 0) return []

  const valuesClause = ids.map((_, i) => `($(id_${i})::uuid)`).join(', ')
  const params: Record<string, string> = {}
  ids.forEach((id, i) => {
    params[`id_${i}`] = id
  })

  const rows = await qx.select(
    `
    WITH id_list (id) AS (VALUES ${valuesClause})
    SELECT id_list.id
    FROM id_list
    WHERE NOT EXISTS (
      SELECT 1 FROM organizations o WHERE o.id = id_list.id
    )
    `,
    params,
  )

  return rows.map((r: { id: string }) => r.id)
}

type OrganizationSummary = Pick<IDbOrganization, 'id' | 'logo'> & {
  name: string
  domain: string
}

export async function findOrganizationByNameOrDomain(
  qx: QueryExecutor,
  { name, domain }: { name?: string; domain?: string },
): Promise<OrganizationSummary | null> {
  if (!name && !domain) {
    return null
  }

  const domainJoin = domain
    ? `
      INNER JOIN "organizationIdentities" oi
        ON oi."organizationId" = o.id
       AND oi.type = 'primary-domain'
       AND oi.verified = true
       AND lower(oi.value) = lower($(domain))`
    : ''

  const filters = ['o."deletedAt" IS NULL']

  if (name) {
    filters.push(
      `trim(lower(o."displayName")) = trim(lower($(name)))`,
      `EXISTS (
        SELECT 1
        FROM "organizationIdentities" oi_check
        WHERE oi_check."organizationId" = o.id
          AND oi_check.type = 'primary-domain'
          AND oi_check.verified = true
      )`,
    )
  }

  const domainSelect = domain
    ? 'lower(oi.value) AS domain'
    : `(
        SELECT lower(oi_domain.value)
        FROM "organizationIdentities" oi_domain
        WHERE oi_domain."organizationId" = o.id
          AND oi_domain.type = 'primary-domain'
          AND oi_domain.verified = true
        ORDER BY lower(oi_domain.value)
        LIMIT 1
      ) AS domain`

  const sql = `
    SELECT
      o.id,
      o."displayName" AS name,
      o.logo,
      ${domainSelect}
    FROM "organizations" o
    ${domainJoin}
    LEFT JOIN "organizationsGlobalActivityCount" gac
      ON gac."organizationId" = o.id
    WHERE
      ${filters.join(' AND ')}
    ORDER BY
      COALESCE(gac.total_count_estimate, 0) DESC,
      (
        SELECT COUNT(DISTINCT mo."memberId")
        FROM "memberOrganizations" mo
        WHERE mo."organizationId" = o.id
          AND mo."deletedAt" IS NULL
      ) DESC,
      o.id
    LIMIT 1;
  `

  return qx.selectOneOrNone(sql, {
    name,
    domain,
  })
}
