import { DEFAULT_TENANT_ID } from '@crowd/common'
import {
  IOrganizationIdentity,
  type OrganizationIdentityDbInsert,
  type OrganizationIdentityDbRow,
  OrganizationIdentityType,
} from '@crowd/types'

import { QueryExecutor } from '../queryExecutor'
import { QueryOptions, QueryResult, prepareBulkInsert, queryTable } from '../utils'

import { IDbOrgIdentityUpdateInput } from './types'

export async function fetchOrgIdentities(
  qx: QueryExecutor,
  organizationId: string,
): Promise<IOrganizationIdentity[]> {
  return qx.select(
    `
      SELECT *
      FROM "organizationIdentities"
      WHERE "organizationId" = $(organizationId)
    `,
    {
      organizationId,
    },
  )
}

export async function fetchManyOrgIdentities(
  qx: QueryExecutor,
  organizationIds: string[],
): Promise<{ organizationId: string; identities: IOrganizationIdentity[] }[]> {
  return qx.select(
    `
      SELECT
          oi."organizationId",
          JSONB_AGG(oi ORDER BY oi."createdAt") AS "identities"
      FROM "organizationIdentities" oi
      WHERE oi."organizationId" IN ($(organizationIds:csv))
      GROUP BY oi."organizationId"
    `,
    {
      organizationIds,
    },
  )
}

export async function cleanUpOrgIdentities(qx: QueryExecutor, organizationId: string) {
  return qx.result(
    `
      DELETE
      FROM "organizationIdentities"
      WHERE "organizationId" = $(organizationId)
    `,
    {
      organizationId,
    },
  )
}

export async function updateOrgIdentityVerifiedFlag(
  qx: QueryExecutor,
  identity: IDbOrgIdentityUpdateInput,
): Promise<void> {
  await qx.result(
    `
    update "organizationIdentities" set verified = $(verified)
    where "organizationId" = $(organizationId) and platform = $(platform) and value = $(value) and type = $(type)
    `,
    identity,
  )
}

export async function insertOrganizationIdentities(
  qx: QueryExecutor,
  identities: OrganizationIdentityDbInsert[],
  failOnConflict: boolean,
  returnRows: true,
): Promise<OrganizationIdentityDbRow[]>
export async function insertOrganizationIdentities(
  qx: QueryExecutor,
  identities: OrganizationIdentityDbInsert[],
  failOnConflict?: boolean,
  returnRows?: false,
): Promise<number>
export async function insertOrganizationIdentities(
  qx: QueryExecutor,
  identities: OrganizationIdentityDbInsert[],
  failOnConflict = false,
  returnRows = false,
): Promise<OrganizationIdentityDbRow[] | number> {
  if (identities.length === 0) {
    return returnRows ? [] : 0
  }

  const query = prepareBulkInsert(
    'organizationIdentities',
    [
      'organizationId',
      'platform',
      'value',
      'type',
      'verified',
      'source',
      'sourceId',
      'tenantId',
      'integrationId',
    ],
    identities.map((i) => ({
      organizationId: i.organizationId,
      platform: i.platform,
      value: i.value,
      type: i.type,
      verified: i.verified,
      source: i.source ?? null,
      sourceId: i.sourceId ?? null,
      tenantId: DEFAULT_TENANT_ID,
      integrationId: i.integrationId ?? null,
    })),
    failOnConflict ? undefined : 'DO NOTHING',
    returnRows,
  )

  if (returnRows) {
    return qx.select(query)
  }

  return qx.result(query)
}

export async function upsertOrgIdentities(
  qe: QueryExecutor,
  organizationId: string,
  identities: Partial<IOrganizationIdentity>[],
  integrationId?: string,
): Promise<void> {
  const existingIdentities = await fetchOrgIdentities(qe, organizationId)
  const toCreate: OrganizationIdentityDbInsert[] = []
  const toUpdate: Partial<IOrganizationIdentity>[] = []

  for (const i of identities) {
    const existing = existingIdentities.find(
      (ei) => ei.value === i.value && ei.platform === i.platform && ei.type === i.type,
    )
    if (!existing) {
      toCreate.push({
        organizationId,
        platform: i.platform,
        type: i.type,
        value: i.value,
        verified: i.verified,
        source: i.source ?? null,
        sourceId: i.sourceId ?? null,
        integrationId: integrationId ?? null,
      })
    } else if (existing.verified !== i.verified) {
      toUpdate.push(i)
    }
  }

  if (toCreate.length > 0) {
    await insertOrganizationIdentities(qe, toCreate, false)
  }

  for (const i of toUpdate) {
    await updateOrgIdentityVerifiedFlag(qe, {
      organizationId,
      platform: i.platform,
      type: i.type,
      value: i.value,
      verified: i.verified,
    })
  }
}

export type OrganizationVerifiedPrimaryDomains = {
  orgId: string
  displayName: string | null
  domains: string[]
}

/**
 * Fetches verified primary domains grouped by organization ID.
 * Returns both names and domains in lowercase to safely match against activity emails.
 */
export async function fetchManyOrganizationVerifiedPrimaryDomains(
  qx: QueryExecutor,
  organizationIds: string[],
): Promise<OrganizationVerifiedPrimaryDomains[]> {
  if (organizationIds.length === 0) {
    return []
  }

  return qx.select(
    `
      SELECT
        oi."organizationId" AS "orgId",
        lower(o."displayName") AS "displayName",
        array_agg(DISTINCT lower(oi.value)) AS domains
      FROM "organizationIdentities" oi
      JOIN organizations o ON o.id = oi."organizationId"
      WHERE oi."organizationId" IN ($(organizationIds:csv))
        AND oi.type = 'primary-domain'
        AND oi.verified = true
      GROUP BY oi."organizationId", o."displayName"
    `,
    { organizationIds },
  )
}

/**
 * Prefer companies over universities (when both exist).
 */
export function preferCompanyOverUniversityWhenOverlapping<T extends { organizationId: string }>(
  candidates: T[],
  orgDomains: OrganizationVerifiedPrimaryDomains[],
): T[] {
  if (candidates.length < 2) {
    return candidates
  }

  // 1. Build a quick lookup map for the organization data
  const orgsById = new Map(orgDomains.map((org) => [org.orgId, org]))

  const companies: T[] = []
  const universities: T[] = []

  // 2. Single pass: Check the heuristic and categorize right here
  for (const candidate of candidates) {
    const org = orgsById.get(candidate.organizationId)

    // If we find the org, check if it's a university based on name or .edu domain
    const isUniversity = org
      ? (org.displayName?.includes('university') ?? false) ||
        org.domains.some((d) => d.endsWith('.edu'))
      : false

    if (isUniversity) {
      universities.push(candidate)
    } else {
      companies.push(candidate)
    }
  }

  // 3. If there's an overlap, prefer companies. Otherwise, return the original list.
  return companies.length > 0 && universities.length > 0 ? companies : candidates
}

export async function findOrgIdByDomain(
  qx: QueryExecutor,
  domains: string[],
): Promise<string | null> {
  const domainIdentityTypes = [
    OrganizationIdentityType.PRIMARY_DOMAIN,
    OrganizationIdentityType.ALTERNATIVE_DOMAIN,
  ]
  const result = await qx.selectOneOrNone(
    `
      SELECT "organizationId"
      FROM "organizationIdentities"
      WHERE "value" = ANY($(domains))
        AND "type" = ANY($(domainIdentityTypes))
      LIMIT 1;
    `,
    {
      domains,
      domainIdentityTypes,
    },
  )

  if (result) {
    return result.organizationId
  }

  return null
}

export enum OrgIdentityField {
  ORGANIZATION_ID = 'organizationId',
  PLATFORM = 'platform',
  SOURCE_ID = 'sourceId',
  INTEGRATION_ID = 'integrationId',
  CREATED_AT = 'createdAt',
  UPDATED_AT = 'updatedAt',
  VERIFIED = 'verified',
  TYPE = 'type',
  VALUE = 'value',
}

export async function queryOrgIdentities<T extends OrgIdentityField>(
  qx: QueryExecutor,
  opts: QueryOptions<T> = {},
): Promise<QueryResult<T>[]> {
  return queryTable(qx, 'organizationIdentities', Object.values(OrgIdentityField), opts)
}
