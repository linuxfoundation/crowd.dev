import { uniq } from 'lodash'

import { getServiceChildLogger } from '@crowd/logging'

import { QueryExecutor } from '../queryExecutor'
import { prepareBulkInsert } from '../utils'
import { IDbOrgAttribute, IDbOrgAttributeInput } from './types'

const log = getServiceChildLogger('organizations/attributes')

export async function findOrgAttributes(
  qx: QueryExecutor,
  organizationId: string,
): Promise<IDbOrgAttribute[]> {
  return qx.select(
    `
    SELECT
      *
    FROM "orgAttributes"
    WHERE "organizationId" = $(organizationId)
  `,
    {
      organizationId,
    },
  )
}

export async function findManyOrgAttributes(
  qx: QueryExecutor,
  organizationIds: string[],
): Promise<{ organizationId: string; attributes: IDbOrgAttribute[] }[]> {
  return qx.select(
    `
      SELECT
        oa."organizationId",
        JSONB_AGG(oa ORDER BY oa."createdAt") AS "attributes"
      FROM "orgAttributes" oa
      WHERE oa."organizationId" IN ($(organizationIds:csv))
      GROUP BY oa."organizationId"
    `,
    {
      organizationIds,
    },
  )
}

export const upsertOrgAttributes = async (
  qx: QueryExecutor,
  organizationId: string,
  attributes: IDbOrgAttributeInput[],
): Promise<void> => {
  if (attributes.length === 0) {
    return
  }

  const objects = uniq(
    attributes.map((a) => {
      return {
        organizationId,
        ...a,
        default: false,
      }
    }),
  )

  try {
    const prepared = prepareBulkInsert(
      'orgAttributes',
      ['organizationId', 'name', 'source', 'default', 'value'],
      objects,
      `("organizationId", name, source, MD5(value)) DO NOTHING`,
    )
    await qx.result(prepared)
  } catch (err) {
    console.error(err)
    log.error('Failed to upsert organization attributes!', err)
    throw err
  }
}

export async function markOrgAttributeDefault(
  qx: QueryExecutor,
  organizationId: string,
  {
    name,
    source,
    value,
  }: {
    name: string
    source: string
    value: string
  },
): Promise<void> {
  try {
    await qx.result(
      `
        UPDATE "orgAttributes"
        SET "default" = CASE
          WHEN source = $(source) AND value = $(value) THEN true
          ELSE false
        END
        WHERE "organizationId" = $(organizationId)
          AND name = $(name)
      `,
      {
        organizationId,
        name,
        source,
        value,
      },
    )
  } catch (err) {
    console.error(err)
    log.error('Failed to upsert organization attributes!', err)
    throw err
  }
}

export async function deleteOrganizationAttributes(
  qx: QueryExecutor,
  ids: string[],
): Promise<void> {
  if (ids.length === 0) {
    return
  }

  await qx.result(
    `
    DELETE FROM "orgAttributes"
    WHERE "organizationId" IN ($(ids:csv))
  `,
    { ids },
  )
}
