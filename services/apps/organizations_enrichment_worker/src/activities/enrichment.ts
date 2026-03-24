import { ApplicationFailure } from '@temporalio/client'
import axios from 'axios'
import uniqBy from 'lodash.uniqby'

import { redactNullByte } from '@crowd/common'
import {
  OrganizationField,
  fetchOrganizationEnrichmentCache,
  findOrgAttributes,
  findOrgById,
  insertOrganizationEnrichmentCache,
  markOrgAttributeDefault,
  prepareOrganizationData,
  setOrganizationEnrichmentCacheUpdatedAt,
  setOrganizationEnrichmentLastTriedAt,
  setOrganizationEnrichmentLastUpdatedAt,
  updateOrganization,
  updateOrganizationEnrichmentCache as updateOrganizationEnrichmentCacheDb,
  upsertOrgAttributes,
  upsertOrgIdentities,
} from '@crowd/data-access-layer'
import { dbStoreQx } from '@crowd/data-access-layer/src/queryExecutor'
import { refreshMaterializedView } from '@crowd/data-access-layer/src/utils'
import {
  IEnrichableOrganization,
  IOrganizationEnrichmentCache,
  OrganizationAttributeSource,
  OrganizationEnrichmentSource,
} from '@crowd/types'

import { OrganizationEnrichmentSourceServiceFactory } from '../factory'
import { svc } from '../main'
import {
  IOrganizationEnrichmentData,
  IOrganizationEnrichmentDataNormalized,
  IOrganizationEnrichmentService,
  IOrganizationEnrichmentSourceInput,
} from '../types'

export async function refreshOrganizationEnrichmentMaterializedView(mvName: string): Promise<void> {
  await refreshMaterializedView(svc.postgres.writer.connection(), mvName)
}

export async function getMaxConcurrentRequests(
  organizations: IEnrichableOrganization[],
  possibleSources: OrganizationEnrichmentSource[],
  concurrencyLimit: number,
): Promise<number> {
  const serviceMap: Partial<Record<OrganizationEnrichmentSource, IOrganizationEnrichmentService>> =
    {}
  const currentProcessingActivityCount = organizations[0].activityCount

  let maxConcurrentRequestsInAllSources = concurrencyLimit

  for (const source of possibleSources) {
    serviceMap[source] = OrganizationEnrichmentSourceServiceFactory.getEnrichmentSourceService(
      source,
      svc.log,
    )

    const activityCountCutoff = serviceMap[source].enrichOrganizationsWithActivityMoreThan
    if (!activityCountCutoff || activityCountCutoff <= currentProcessingActivityCount) {
      maxConcurrentRequestsInAllSources = Math.min(
        maxConcurrentRequestsInAllSources,
        serviceMap[source].maxConcurrentRequests,
      )
    }
  }

  svc.log.info('Setting max concurrent requests', { maxConcurrentRequestsInAllSources })

  return maxConcurrentRequestsInAllSources
}

export async function createOrganizationEnrichmentCache(
  source: OrganizationEnrichmentSource,
  organizationId: string,
  data: IOrganizationEnrichmentData,
): Promise<void> {
  const qx = dbStoreQx(svc.postgres.writer)
  const dataSanitized = data ? redactNullByte(JSON.stringify(data)) : null
  await insertOrganizationEnrichmentCache(qx, organizationId, dataSanitized, source)
}

export async function findOrganizationEnrichmentCache(
  source: OrganizationEnrichmentSource[],
  organizationId: string,
): Promise<IOrganizationEnrichmentCache<IOrganizationEnrichmentData>[]> {
  const qx = dbStoreQx(svc.postgres.reader)
  return fetchOrganizationEnrichmentCache(qx, organizationId, source)
}

export async function updateOrganizationEnrichmentCache(
  source: OrganizationEnrichmentSource,
  organizationId: string,
  data: IOrganizationEnrichmentData,
): Promise<void> {
  const qx = dbStoreQx(svc.postgres.writer)
  const dataSanitized = data ? redactNullByte(JSON.stringify(data)) : null
  await updateOrganizationEnrichmentCacheDb(qx, organizationId, dataSanitized, source)
}

export async function touchOrganizationEnrichmentLastTriedAt(
  organizationId: string,
): Promise<void> {
  const qx = dbStoreQx(svc.postgres.writer)
  await setOrganizationEnrichmentLastTriedAt(qx, organizationId)
}

export async function touchOrganizationEnrichmentCacheUpdatedAt(
  source: OrganizationEnrichmentSource,
  organizationId: string,
): Promise<void> {
  const qx = dbStoreQx(svc.postgres.writer)
  await setOrganizationEnrichmentCacheUpdatedAt(qx, organizationId, source)
}

export async function isCacheObsolete(
  source: OrganizationEnrichmentSource,
  cache: IOrganizationEnrichmentCache<IOrganizationEnrichmentData>,
): Promise<boolean> {
  const service = OrganizationEnrichmentSourceServiceFactory.getEnrichmentSourceService(
    source,
    svc.log,
  )

  if (!cache) return true

  if (service.neverReenrich) return false

  if (service.cacheObsoleteAfterSeconds === undefined) {
    throw new Error(
      `"${source}" requires cacheObsoleteAfterSeconds when neverReenrich is false or undefined`,
    )
  }

  return Date.now() - new Date(cache.updatedAt).getTime() > 1000 * service.cacheObsoleteAfterSeconds
}

export async function getEnrichmentInput(
  input: IEnrichableOrganization,
): Promise<IOrganizationEnrichmentSourceInput> {
  // dedup domains by value (keep first occurrence)
  const uniqueDomains = uniqBy(input.identities, 'value')

  return {
    organizationId: input.id,
    domains: uniqueDomains,
    displayName: input.displayName,
    activityCount: input.activityCount || 0,
  }
}

export async function getEnrichmentData(
  source: OrganizationEnrichmentSource,
  input: IOrganizationEnrichmentSourceInput,
): Promise<IOrganizationEnrichmentData | null> {
  const service = OrganizationEnrichmentSourceServiceFactory.getEnrichmentSourceService(
    source,
    svc.log,
  )

  if (await service.isEnrichableBySource(input)) {
    try {
      return await service.getData(input)
    } catch (err) {
      if (axios.isAxiosError(err)) {
        const status = err.response?.status

        if (status === 401 || status === 403) {
          throw ApplicationFailure.nonRetryable(
            `Invalid ${source} credentials or permissions`,
            `${source.toUpperCase()}_AUTH_ERROR`,
          )
        }

        if (status === 400) {
          svc.log.error({ source, status, input }, 'Bad request: invalid input data')
          throw ApplicationFailure.nonRetryable(
            `Bad request to ${source}: invalid input`,
            `${source.toUpperCase()}_BAD_REQUEST`,
          )
        }
      }

      svc.log.error({ source, err }, 'Error getting enrichment data!')
      throw err
    }
  }

  return null
}

export async function normalizeEnrichmentData(
  source: OrganizationEnrichmentSource,
  data: IOrganizationEnrichmentData,
): Promise<IOrganizationEnrichmentDataNormalized> {
  const service = OrganizationEnrichmentSourceServiceFactory.getEnrichmentSourceService(
    source,
    svc.log,
  )
  return service.normalize(data)
}

export async function applyEnrichmentToOrganization(
  organizationId: string,
  data: IOrganizationEnrichmentDataNormalized,
): Promise<void> {
  const qx = dbStoreQx(svc.postgres.writer)
  const orgData = await findOrgById(qx, organizationId, [
    OrganizationField.ID,
    OrganizationField.DISPLAY_NAME,
    OrganizationField.IS_TEAM_ORGANIZATION,
    OrganizationField.MANUALLY_CREATED,
  ])

  const existingAttributes = await findOrgAttributes(qx, organizationId)

  const prepared = prepareOrganizationData(
    // organization attributes are expected to be flattened
    { identities: data.identities, displayName: data.displayName, ...data.attributes },
    OrganizationAttributeSource.ENRICHMENT_LFX_INTERNAL_API,
    orgData,
    existingAttributes,
  )

  await svc.postgres.writer.transactionally(async (t) => {
    const txQe = dbStoreQx(t)

    // update organization
    await updateOrganization(txQe, organizationId, prepared.organization)

    // upsert organization attributes
    await upsertOrgAttributes(txQe, organizationId, prepared.attributes)

    // mark default attributes
    const defaultAttrs = prepared.attributes.filter((a) => a.default)
    await Promise.all(
      defaultAttrs.map((attr) => markOrgAttributeDefault(txQe, organizationId, attr)),
    )

    // upsert organization identities
    await upsertOrgIdentities(
      txQe,
      organizationId,
      data.identities.map((i) => ({ ...i, source: 'enrichment' })),
    )
  })

  await setOrganizationEnrichmentLastUpdatedAt(qx, organizationId)
}
