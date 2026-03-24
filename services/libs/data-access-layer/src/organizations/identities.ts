import { DEFAULT_TENANT_ID } from '@crowd/common'
import {
  IOrganizationIdentity,
  NewOrganizationIdentity,
  OrganizationIdentityType,
} from '@crowd/types'

import { QueryExecutor } from '../queryExecutor'
import { QueryOptions, QueryResult, queryTable } from '../utils'

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

export async function addOrgIdentity(qx: QueryExecutor, identity: NewOrganizationIdentity) {
  return qx.result(
    `
      INSERT INTO "organizationIdentities" (
        "organizationId",
        "source",
        platform,
        value,
        type,
        verified,
        "sourceId",
        "tenantId",
        "integrationId",
        "createdAt"
      )
      VALUES ($(organizationId), $(source), $(platform), $(value), $(type), $(verified), $(sourceId), $(tenantId), $(integrationId), NOW())
      ON CONFLICT DO NOTHING;
    `,
    { tenantId: DEFAULT_TENANT_ID, ...identity },
  )
}

export async function upsertOrgIdentities(
  qe: QueryExecutor,
  organizationId: string,
  identities: Partial<IOrganizationIdentity>[],
  integrationId?: string,
): Promise<void> {
  const existingIdentities = await fetchOrgIdentities(qe, organizationId)
  const toCreate = []
  const toUpdate = []

  for (const i of identities) {
    const existing = existingIdentities.find(
      (ei) => ei.value === i.value && ei.platform === i.platform && ei.type === i.type,
    )
    if (!existing) {
      toCreate.push(i)
    } else if (existing && existing.verified !== i.verified) {
      toUpdate.push(i)
    }
  }

  if (toCreate.length > 0) {
    for (const i of toCreate) {
      // add the identity
      await addOrgIdentity(qe, {
        organizationId,
        platform: i.platform,
        type: i.type,
        value: i.value,
        verified: i.verified,
        source: i.source,
        sourceId: i.sourceId,
        integrationId,
      })
    }
  }

  if (toUpdate.length > 0) {
    for (const i of toUpdate) {
      // update the identity
      await updateOrgIdentityVerifiedFlag(qe, {
        organizationId,
        platform: i.platform,
        type: i.type,
        value: i.value,
        verified: i.verified,
      })
    }
  }
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
