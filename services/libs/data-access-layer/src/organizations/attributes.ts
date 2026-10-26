import { uniq } from 'lodash'

import { getServiceChildLogger } from '@crowd/logging'
import { OrganizationAttributeSource } from '@crowd/types'

import { QueryExecutor } from '../queryExecutor'
import { prepareBulkInsert } from '../utils'
import { ORG_DB_ATTRIBUTES, ORG_DB_ATTRIBUTE_SOURCE_PRIORITY } from './attributesConfig'
import { IDbOrgAttribute, IDbOrgAttributeInput, IDbOrganizationInput } from './types'
import { toDefaultColumnValue } from './utils'

const log = getServiceChildLogger('organizations/attributes')

const sourceRank = (source: string): number => {
  const rank = ORG_DB_ATTRIBUTE_SOURCE_PRIORITY.indexOf(source as OrganizationAttributeSource)
  return rank === -1 ? ORG_DB_ATTRIBUTE_SOURCE_PRIORITY.length : rank
}

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

export async function moveOrgAttributes(
  qx: QueryExecutor,
  fromOrganizationId: string,
  toOrganizationId: string,
  excludedNames: string[],
): Promise<string[]> {
  const moved: { name: string }[] = await qx.select(
    `
      UPDATE "orgAttributes" moved
      SET
        "organizationId" = $(toOrganizationId),
        "default" = moved."default" AND NOT EXISTS (
          SELECT 1
          FROM "orgAttributes" existing
          WHERE existing."organizationId" = $(toOrganizationId)
            AND existing.name = moved.name
            AND existing.source = moved.source
            AND existing."default"
        ),
        "updatedAt" = NOW()
      WHERE moved."organizationId" = $(fromOrganizationId)
        AND moved.name <> ALL($(excludedNames)::text[])
        AND NOT EXISTS (
          SELECT 1
          FROM "orgAttributes" existing
          WHERE existing."organizationId" = $(toOrganizationId)
            AND existing.name = moved.name
            AND existing.source = moved.source
            AND existing.value = moved.value
        )
      RETURNING moved.name
    `,
    { fromOrganizationId, toOrganizationId, excludedNames },
  )

  return uniq(moved.map(({ name }) => name))
}

export async function deleteOrgAttributes(
  qx: QueryExecutor,
  organizationId: string,
  attributes: IDbOrgAttributeInput[],
): Promise<string[]> {
  if (attributes.length === 0) {
    return []
  }

  const deleted: { name: string }[] = await qx.select(
    `
      DELETE FROM "orgAttributes"
      WHERE "organizationId" = $(organizationId)
        AND (name, source, value) IN (
          SELECT * FROM UNNEST($(names)::text[], $(sources)::text[], $(values)::text[])
        )
      RETURNING name
    `,
    {
      organizationId,
      names: attributes.map(({ name }) => name),
      sources: attributes.map(({ source }) => source),
      values: attributes.map(({ value }) => value),
    },
  )

  return uniq(deleted.map(({ name }) => name))
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

export function pickDefaultAttribute(attributes: IDbOrgAttribute[]): IDbOrgAttribute | undefined {
  const [picked] = [...attributes].sort(
    (a, b) => sourceRank(a.source) - sourceRank(b.source) || Number(b.default) - Number(a.default),
  )
  return picked
}

export async function recalculateOrgAttributeDefaults(
  qx: QueryExecutor,
  organizationId: string,
  names: string[],
): Promise<Partial<IDbOrganizationInput>> {
  if (names.length === 0) {
    return {}
  }

  const attributes: IDbOrgAttribute[] = await qx.select(
    `
      SELECT id, name, source, value, "default"
      FROM "orgAttributes"
      WHERE "organizationId" = $(organizationId)
        AND name IN ($(names:csv))
      ORDER BY "createdAt" DESC
    `,
    { organizationId, names },
  )

  const defaults = new Map(
    names.map((name) => [name, pickDefaultAttribute(attributes.filter((a) => a.name === name))]),
  )

  await qx.result(
    `
      UPDATE "orgAttributes"
      SET "default" = (id = ANY($(defaultIds)::uuid[]))
      WHERE "organizationId" = $(organizationId)
        AND name IN ($(names:csv))
        AND "default" IS DISTINCT FROM (id = ANY($(defaultIds)::uuid[]))
    `,
    {
      organizationId,
      names,
      defaultIds: [...defaults.values()].filter(Boolean).map(({ id }) => id),
    },
  )

  const columns: Record<string, unknown> = {}
  for (const definition of ORG_DB_ATTRIBUTES) {
    if (definition.defaultColumn && defaults.has(definition.name)) {
      const picked = defaults.get(definition.name)
      columns[definition.defaultColumn] = picked
        ? toDefaultColumnValue(definition, picked.value)
        : null
    }
  }

  const columnNames = Object.keys(columns)
  if (columnNames.length > 0) {
    await qx.result(
      `
        UPDATE organizations
        SET ${columnNames.map((column) => `"${column}" = $(columns.${column})`).join(', ')},
          "updatedAt" = NOW()
        WHERE id = $(organizationId)
      `,
      { organizationId, columns },
    )
  }

  return columns as Partial<IDbOrganizationInput>
}
