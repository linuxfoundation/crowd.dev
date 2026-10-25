import {
  IEnrichableOrganization,
  IEnrichmentSourceQueryInput,
  OrganizationEnrichmentSource,
} from '@crowd/types'

import { QueryExecutor } from '../../queryExecutor'

export * from './cache'

export async function fetchOrganizationsForEnrichment(
  qx: QueryExecutor,
  limit: number,
  sourceInputs: IEnrichmentSourceQueryInput<OrganizationEnrichmentSource>[],
): Promise<IEnrichableOrganization[]> {
  const cacheAgeInnerQueryItems = []
  const enrichableBySqlConditions = []

  sourceInputs.forEach((input) => {
    if (input.neverReenrich) {
      cacheAgeInnerQueryItems.push(
        `
          ( NOT EXISTS (
            SELECT 1 FROM "organizationEnrichmentCache" oec
            WHERE oec."organizationId" = o.id
            AND oec.source = '${input.source}')
          )`,
      )
    } else {
      cacheAgeInnerQueryItems.push(
        `
          ( NOT EXISTS (
            SELECT 1 FROM "organizationEnrichmentCache" oec
            WHERE oec."organizationId" = o.id
            AND oec.source = '${input.source}'
            AND EXTRACT(EPOCH FROM (now() - oec."updatedAt")) < ${input.cacheObsoleteAfterSeconds})
          )`,
      )
    }

    enrichableBySqlConditions.push(`(${input.enrichableBySql})`)
  })

  let enrichableBySqlJoined = ''

  if (enrichableBySqlConditions.length > 0) {
    enrichableBySqlJoined = `(${enrichableBySqlConditions.join(' OR ')}) `
  }

  return qx.select(
    `
        SELECT
             o."id",
             o."displayName",
              JSON_AGG(
                JSON_BUILD_OBJECT(
                  'platform', oi.platform,
                  'value', oi.value,
                  'type', oi.type,
                  'verified', oi.verified
                )
              ) AS identities,
              MAX(coalesce("organizationsGlobalActivityCount".total_count_estimate, 0)) AS "activityCount"
        FROM organizations o
             INNER JOIN "organizationIdentities" oi ON oi."organizationId" = o.id AND oi.verified = true AND oi.type = 'primary-domain'
             LEFT JOIN "organizationsGlobalActivityCount" ON "organizationsGlobalActivityCount"."organizationId" = o.id
        WHERE
          ${enrichableBySqlJoined}
          AND o."deletedAt" IS NULL
          AND (${cacheAgeInnerQueryItems.join(' OR ')})
        GROUP BY o.id
        ORDER BY "activityCount" DESC
        LIMIT $(limit);
        `,
    { limit },
  )
}

export async function setOrganizationEnrichmentLastTriedAt(
  qx: QueryExecutor,
  organizationId: string,
): Promise<void> {
  await qx.result(
    `
    insert into "organizationEnrichments"("organizationId", "lastTriedAt")
    values ($(organizationId), now())
    on conflict ("organizationId") do update set "lastTriedAt" = now()
    `,
    { organizationId },
  )
}

export async function setOrganizationEnrichmentLastUpdatedAt(
  qx: QueryExecutor,
  organizationId: string,
): Promise<void> {
  await qx.result(
    `
    insert into "organizationEnrichments"("organizationId", "lastTriedAt", "lastUpdatedAt")
    values ($(organizationId), now(), now())
    on conflict ("organizationId") do update set "lastUpdatedAt" = now()
    `,
    { organizationId },
  )
}

export async function deleteOrganizationEnrichment(
  qx: QueryExecutor,
  organizationId: string,
): Promise<void> {
  await qx.tx(async (tx) => {
    await tx.result(
      `delete from "organizationEnrichments" where "organizationId" = $(organizationId)`,
      { organizationId },
    )
    await tx.result(
      `delete from "organizationEnrichmentCache" where "organizationId" = $(organizationId)`,
      { organizationId },
    )
  })
}
