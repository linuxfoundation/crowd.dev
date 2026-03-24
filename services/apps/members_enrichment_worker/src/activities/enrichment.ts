import { ApplicationFailure } from '@temporalio/client'
import axios from 'axios'
import _ from 'lodash'

import {
  generateUUIDv1,
  hasIntersection,
  replaceDoubleQuotes,
  setAttributesDefaultValues,
} from '@crowd/common'
import {
  changeMemberOrganizationAffiliationOverrides,
  checkOrganizationAffiliationPolicy,
  updateMemberAttributes,
  updateMemberContributions,
  updateMemberReach,
} from '@crowd/data-access-layer'
import { createMemberIdentity } from '@crowd/data-access-layer'
import { findMemberIdentityWithTheMostActivityInPlatform as getMemberMostActiveIdentity } from '@crowd/data-access-layer/src/activityRelations'
import { getPlatformPriorityArray } from '@crowd/data-access-layer/src/members/attributeSettings'
import {
  deleteMemberOrgById,
  fetchMemberDataForLLMSquashing as fetchMemberDataForLLMSquashingDb,
  findMemberEnrichmentCacheDb,
  findMemberEnrichmentCacheForAllSourcesDb,
  insertMemberEnrichmentCacheDb,
  insertWorkExperience,
  setMemberEnrichmentLastTriedAt,
  setMemberEnrichmentUpdatedAt,
  touchMemberEnrichmentCacheUpdatedAtDb,
  updateMemberEnrichmentCacheDb,
  updateMemberOrg,
} from '@crowd/data-access-layer/src/old/apps/members_enrichment_worker'
import { findOrCreateOrganization } from '@crowd/data-access-layer/src/organizations'
import { dbStoreQx, pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { refreshMaterializedView } from '@crowd/data-access-layer/src/utils'
import { SearchSyncApiClient } from '@crowd/opensearch'
import { RedisCache } from '@crowd/redis'
import {
  IEnrichableMember,
  IEnrichableMemberIdentityActivityAggregate,
  IMemberEnrichmentCache,
  IMemberOrganizationData,
  IMemberOriginalData,
  IMemberReach,
  MemberEnrichmentSource,
  MemberIdentityType,
  OrganizationAttributeSource,
  OrganizationIdentityType,
  OrganizationSource,
  PlatformType,
} from '@crowd/types'

import { EnrichmentSourceServiceFactory } from '../factory'
import { svc } from '../service'
import {
  IEnrichmentService,
  IEnrichmentSourceInput,
  IMemberEnrichmentData,
  IMemberEnrichmentDataNormalized,
  IMemberEnrichmentDataNormalizedOrganization,
} from '../types'

/* eslint-disable @typescript-eslint/no-explicit-any */

// Get the most strict parallelism among existing and enrichable sources
// We only check sources that has activity count cutoff in current range
export async function getMaxConcurrentRequests(
  members: IEnrichableMember[],
  possibleSources: MemberEnrichmentSource[],
  concurrencyLimit: number,
): Promise<number> {
  const serviceMap: Partial<Record<MemberEnrichmentSource, IEnrichmentService>> = {}
  const currentProcessingActivityCount = members[0].activityCount

  let maxConcurrentRequestsInAllSources = concurrencyLimit

  for (const source of possibleSources) {
    serviceMap[source] = EnrichmentSourceServiceFactory.getEnrichmentSourceService(source, svc.log)
    const activityCountCutoff = serviceMap[source].enrichMembersWithActivityMoreThan
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

export async function getEnrichmentData(
  source: MemberEnrichmentSource,
  input: IEnrichmentSourceInput,
): Promise<IMemberEnrichmentData | null> {
  const service = EnrichmentSourceServiceFactory.getEnrichmentSourceService(source, svc.log)

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

export async function getEnrichmentInput(
  input: IEnrichableMember,
): Promise<IEnrichmentSourceInput> {
  const enrichmentInput: IEnrichmentSourceInput = {
    memberId: input.id,
    email: input.identities.find((i) => i.verified && i.type === MemberIdentityType.EMAIL),
    linkedin: input.identities.find(
      (i) =>
        i.verified &&
        i.platform === PlatformType.LINKEDIN &&
        i.type === MemberIdentityType.USERNAME,
    ),
    github: input.identities.find(
      (i) =>
        i.verified && i.platform === PlatformType.GITHUB && i.type === MemberIdentityType.USERNAME,
    ),
    displayName: input.displayName || undefined,
    website: input.website || undefined,
    location: input.location || undefined,
    activityCount: input.activityCount || 0,
  }

  return enrichmentInput
}

export async function normalizeEnrichmentData(
  source: MemberEnrichmentSource,
  data: IMemberEnrichmentData,
): Promise<IMemberEnrichmentDataNormalized | IMemberEnrichmentDataNormalized[]> {
  const service = EnrichmentSourceServiceFactory.getEnrichmentSourceService(source, svc.log)
  return service.normalize(data)
}

export async function isCacheObsolete(
  source: MemberEnrichmentSource,
  cache: IMemberEnrichmentCache<IMemberEnrichmentData>,
): Promise<boolean> {
  return isCacheObsoleteSync(source, cache)
}

export function isCacheObsoleteSync(
  source: MemberEnrichmentSource,
  cache: IMemberEnrichmentCache<IMemberEnrichmentData>,
): boolean {
  const service = EnrichmentSourceServiceFactory.getEnrichmentSourceService(source, svc.log)

  if (!cache) return true

  if (service.neverReenrich) return false

  if (service.cacheObsoleteAfterSeconds === undefined) {
    throw new Error(
      `"${source}" requires cacheObsoleteAfterSeconds when neverReenrich is false or undefined`,
    )
  }
  return Date.now() - new Date(cache.updatedAt).getTime() > 1000 * service.cacheObsoleteAfterSeconds
}

export async function setHasRemainingCredits(
  source: MemberEnrichmentSource,
  hasCredits: boolean,
): Promise<void> {
  const redisCache = new RedisCache(`member-enrichment-${source}`, svc.redis, svc.log)
  if (hasCredits) {
    await redisCache.set('hasRemainingCredits', 'true', 60)
  } else {
    await redisCache.set('hasRemainingCredits', 'false', 60)
  }
}

export async function hasRemainingCredits(source: MemberEnrichmentSource): Promise<boolean> {
  const service = EnrichmentSourceServiceFactory.getEnrichmentSourceService(source, svc.log)
  const redisCache = new RedisCache(`member-enrichment-${source}`, svc.redis, svc.log)

  // read cached value directly (handles missing or expired keys)
  const cachedValue = await redisCache.get('hasRemainingCredits')

  // return cached result when available
  if (cachedValue !== null) {
    return cachedValue === 'true'
  }

  svc.log.debug({ source }, 'hasRemainingCredits not found in cache; refreshing from service!')

  // refresh from service and update cache
  const hasCredits = await service.hasRemainingCredits()
  await setHasRemainingCredits(source, hasCredits)

  return hasCredits
}

export async function findMemberEnrichmentCache(
  sources: MemberEnrichmentSource[],
  memberId: string,
): Promise<IMemberEnrichmentCache<IMemberEnrichmentData>[]> {
  return findMemberEnrichmentCacheDb(svc.postgres.reader.connection(), memberId, sources)
}

export async function getPriorityArray(): Promise<string[]> {
  return getPlatformPriorityArray(dbStoreQx(svc.postgres.reader))
}

export async function fetchMemberDataForLLMSquashing(
  memberId: string,
): Promise<IMemberOriginalData> {
  return fetchMemberDataForLLMSquashingDb(svc.postgres.reader.connection(), memberId)
}

export async function findMemberEnrichmentCacheForAllSources(
  memberId: string,
  returnRowsWithoutData = false,
): Promise<IMemberEnrichmentCache<IMemberEnrichmentData>[]> {
  return findMemberEnrichmentCacheForAllSourcesDb(
    svc.postgres.reader.connection(),
    memberId,
    returnRowsWithoutData,
  )
}

export async function insertMemberEnrichmentCache(
  source: MemberEnrichmentSource,
  memberId: string,
  data: IMemberEnrichmentData,
): Promise<void> {
  await insertMemberEnrichmentCacheDb(svc.postgres.writer.connection(), data, memberId, source)
}

export async function updateMemberEnrichmentCache(
  source: MemberEnrichmentSource,
  memberId: string,
  data: IMemberEnrichmentData,
): Promise<void> {
  await updateMemberEnrichmentCacheDb(svc.postgres.writer.connection(), data, memberId, source)
}

export async function touchMemberEnrichmentLastTriedAt(memberId: string): Promise<void> {
  await setMemberEnrichmentLastTriedAt(svc.postgres.writer.connection(), memberId)
}

export async function touchMemberEnrichmentCacheUpdatedAt(
  source: MemberEnrichmentSource,
  memberId: string,
): Promise<void> {
  await touchMemberEnrichmentCacheUpdatedAtDb(svc.postgres.writer.connection(), memberId, source)
}

export async function findMemberIdentityWithTheMostActivityInPlatform(
  memberId: string,
  platform: string,
): Promise<IEnrichableMemberIdentityActivityAggregate> {
  return getMemberMostActiveIdentity(pgpQx(svc.postgres.reader.connection()), memberId, platform)
}

export async function updateMemberUsingSquashedPayload(
  memberId: string,
  existingMemberData: IMemberOriginalData,
  squashedPayload: IMemberEnrichmentDataNormalized,
  hasContributions: boolean,
  isHighConfidenceSourceSelectedForWorkExperiences: boolean,
): Promise<boolean> {
  return await svc.postgres.writer.transactionally(async (tx) => {
    let updated = false
    const qx = dbStoreQx(tx)

    // process identities
    if (squashedPayload.identities.length > 0) {
      svc.log.debug({ memberId }, 'Adding to member identities!')
      for (const i of squashedPayload.identities) {
        updated = true
        await createMemberIdentity(
          qx,
          {
            memberId,
            platform: i.platform,
            type: i.type,
            value: i.value,
            verified: i.verified,
            source: 'enrichment',
          },
          true,
        )
      }
    }

    // process contributions
    // if squashed payload has data from progai, we should fetch contributions here
    // it's ommited from the payload because it takes a lot of space
    svc.log.debug('Processing contributions! ', { memberId, hasContributions })
    if (hasContributions) {
      const caches = await findMemberEnrichmentCache([MemberEnrichmentSource.PROGAI], memberId)
      if (caches?.length > 0 && caches[0]?.data) {
        const progaiService = EnrichmentSourceServiceFactory.getEnrichmentSourceService(
          MemberEnrichmentSource.PROGAI,
          svc.log,
        )
        const normalized = progaiService.normalize(caches[0].data)
        if (normalized) {
          const typed = normalized as IMemberEnrichmentDataNormalized

          if (typed.contributions) {
            updated = true
            await updateMemberContributions(qx, memberId, typed.contributions)
          }
        }
      }
    }

    // process attributes
    let attributes = existingMemberData.attributes as Record<string, unknown>

    if (squashedPayload.attributes) {
      svc.log.debug({ memberId }, 'Updating member attributes!')

      attributes = _.merge({}, attributes, squashedPayload.attributes)

      if (Object.keys(attributes).length > 0) {
        const priorities = await getPriorityArray()
        attributes = await setAttributesDefaultValues(attributes, priorities)
      }
      updated = true
      await updateMemberAttributes(qx, memberId, attributes)
    }

    // process reach
    if (squashedPayload.reach && Object.keys(squashedPayload.reach).length > 0) {
      svc.log.debug({ memberId }, 'Updating member reach!')
      let reach: IMemberReach

      if (existingMemberData.reach && existingMemberData.reach.total) {
        let total = existingMemberData.reach.total === -1 ? 0 : existingMemberData.reach.total
        for (const reachSource of Object.keys(squashedPayload.reach)) {
          total += squashedPayload.reach[reachSource]
        }
        reach = {
          ...existingMemberData.reach,
          ...squashedPayload.reach,
          total,
        }

        updated = true
        await updateMemberReach(qx, memberId, reach)
      }
    }

    const orgIdsToSync: string[] = []
    const newOrUpdatedMemberOrgs = []

    if (squashedPayload.memberOrganizations.length > 0) {
      const orgPromises = []

      // try matching member's existing organizations with the new ones
      // we'll be using displayName, title, dates
      for (const org of squashedPayload.memberOrganizations) {
        if (!org.organizationId) {
          // Check if any similar in existing work experiences
          const existingOrg = existingMemberData.organizations.find((o) =>
            doesIncomingOrgExistInExistingOrgs(o, org),
          )

          if (existingOrg) {
            // Get all orgs with the same name as the current one
            const matchingOrgs = squashedPayload.memberOrganizations.filter(
              (otherOrg) => otherOrg.name === org.name,
            )

            // Set organizationId for all matching orgs
            for (const matchingOrg of matchingOrgs) {
              matchingOrg.organizationId = existingOrg.orgId
            }
          }
        }
      }

      for (const org of squashedPayload.memberOrganizations.filter((o) => !o.organizationId)) {
        // Skip organizations that don't have a name or any verified identities
        const identities = org.identities ? org.identities : []
        const verifiedIdentities = identities.filter((i) => i.verified)

        if (!org.name && verifiedIdentities.length === 0) {
          svc.log.debug(
            { orgId: org.organizationId },
            'Skipping organization without name or verified identities',
          )
          continue
        }

        const orgSource = OrganizationAttributeSource.ENRICHMENT

        orgPromises.push(
          findOrCreateOrganization(qx, orgSource, {
            displayName: org.name,
            description: org.organizationDescription,
            identities: identities.map((i) => ({ ...i, source: orgSource })),
          })
            .then((orgId) => {
              // set the organization id for later use
              org.organizationId = orgId
              if (org.identities) {
                for (const i of org.identities) {
                  i.organizationId = orgId
                }
              }
              if (orgId) {
                orgIdsToSync.push(orgId)
              }
            })
            .then(() =>
              Promise.all(
                orgIdsToSync.map((orgId) =>
                  syncOrganization(orgId).catch((error) => {
                    console.error(`Failed to sync organization with ID ${orgId}:`, error)
                  }),
                ),
              ),
            ),
        )
      }

      await Promise.all(orgPromises)
      // ignore all organizations that were not created
      squashedPayload.memberOrganizations = squashedPayload.memberOrganizations.filter(
        (o) => o.organizationId,
      )

      const results = prepareWorkExperiences(
        existingMemberData.organizations,
        squashedPayload.memberOrganizations,
        isHighConfidenceSourceSelectedForWorkExperiences,
      )

      if (results.toDelete.length > 0) {
        for (const org of results.toDelete) {
          updated = true
          await deleteMemberOrgById(tx.transaction(), org.id)
        }
      }

      if (results.toCreate.length > 0) {
        for (const org of results.toCreate) {
          if (!org.organizationId) {
            throw new Error('Organization ID is missing!')
          }
          updated = true

          const newMemberOrgId = await insertWorkExperience(
            tx.transaction(),
            memberId,
            org.organizationId,
            org.title,
            org.startDate,
            org.endDate,
            org.source,
          )

          if (newMemberOrgId) {
            newOrUpdatedMemberOrgs.push({
              id: newMemberOrgId,
              organizationId: org.organizationId,
            })
          }
        }
      }

      if (results.toUpdate.size > 0) {
        for (const [memberOrg, toUpdate] of results.toUpdate) {
          updated = true
          const updatedMemberOrgId = await updateMemberOrg(
            tx.transaction(),
            memberId,
            memberOrg,
            toUpdate,
          )

          if (updatedMemberOrgId) {
            newOrUpdatedMemberOrgs.push({
              id: updatedMemberOrgId,
              organizationId: memberOrg.orgId,
            })
          }
        }
      }

      for (const mo of newOrUpdatedMemberOrgs) {
        const isOrganizationAffiliationBlocked = await checkOrganizationAffiliationPolicy(
          qx,
          mo.organizationId,
        )

        if (isOrganizationAffiliationBlocked) {
          await changeMemberOrganizationAffiliationOverrides(qx, [
            {
              memberId,
              memberOrganizationId: mo.id,
              allowAffiliation: false,
            },
          ])
        }
      }
    }

    if (updated) {
      await setMemberEnrichmentUpdatedAt(tx.transaction(), memberId)
      await syncMember(memberId)
    } else {
      await setMemberEnrichmentLastTriedAt(tx.transaction(), memberId)
    }

    svc.log.debug({ memberId }, 'Member sources processed successfully!')

    return updated
  })
}

export function doesIncomingOrgExistInExistingOrgs(
  existingOrg: IMemberOrganizationData,
  incomingOrg: IMemberEnrichmentDataNormalizedOrganization,
): boolean {
  // Check if any similar in existing work experiences
  const incomingVerifiedPrimaryDomainIdentityValues = incomingOrg.identities
    .filter((i) => i.type === OrganizationIdentityType.PRIMARY_DOMAIN && i.verified)
    .map((i) => i.value)

  const existingVerifiedPrimaryDomainIdentityValues = existingOrg.identities
    .filter((i) => i.type === OrganizationIdentityType.PRIMARY_DOMAIN && i.verified)
    .map((i) => i.value)

  const incomingOrgStartDate = incomingOrg.startDate ? new Date(incomingOrg.startDate) : null
  const incomingOrgEndDate = incomingOrg.endDate ? new Date(incomingOrg.endDate) : null
  const existingOrgStartDate = existingOrg.dateStart ? new Date(existingOrg.dateStart) : null
  const existingOrgEndEndDate = existingOrg.dateEnd ? new Date(existingOrg.dateEnd) : null

  const isSameStartMonthYear =
    (!incomingOrgStartDate && !existingOrgStartDate) || // Both start dates are null
    (incomingOrgStartDate &&
      existingOrgStartDate &&
      incomingOrgStartDate.getMonth() === existingOrgStartDate.getMonth() &&
      incomingOrgStartDate.getFullYear() === existingOrgStartDate.getFullYear())

  const isSameEndMonthYear =
    (!incomingOrgEndDate && !existingOrgEndEndDate) || // Both end dates are null
    (incomingOrgEndDate &&
      existingOrgEndEndDate &&
      incomingOrgEndDate.getMonth() === existingOrgEndEndDate.getMonth() &&
      incomingOrgEndDate.getFullYear() === existingOrgEndEndDate.getFullYear())

  return (
    hasIntersection(
      incomingVerifiedPrimaryDomainIdentityValues,
      existingVerifiedPrimaryDomainIdentityValues,
    ) ||
    ((existingOrg.orgName.toLowerCase().includes(incomingOrg.name.toLowerCase()) ||
      incomingOrg.name.toLowerCase().includes(existingOrg.orgName.toLowerCase())) &&
      ((isSameStartMonthYear && isSameEndMonthYear) || incomingOrg.title === existingOrg.jobTitle))
  )
}

export async function getObsoleteSourcesOfMember(
  memberId: string,
  possibleSources: MemberEnrichmentSource[],
): Promise<MemberEnrichmentSource[]> {
  const caches = await findMemberEnrichmentCacheForAllSources(memberId, true)
  const obsoleteSources = possibleSources.filter((source) =>
    isCacheObsoleteSync(
      source,
      caches.find((c) => c.source === source),
    ),
  )
  return obsoleteSources
}

export async function refreshMemberEnrichmentMaterializedView(mvName: string): Promise<void> {
  await refreshMaterializedView(svc.postgres.writer.connection(), mvName)
}

interface IWorkExperienceChanges {
  toDelete: IMemberOrganizationData[]
  toCreate: IMemberEnrichmentDataNormalizedOrganization[]
  toUpdate: Map<IMemberOrganizationData, Record<string, any>>
}

function prepareWorkExperiences(
  oldVersion: IMemberOrganizationData[],
  newVersion: IMemberEnrichmentDataNormalizedOrganization[],
  isHighConfidenceSourceSelectedForWorkExperiences: boolean,
): IWorkExperienceChanges {
  // we delete all the work experiences that were not manually created
  const toDelete = oldVersion.filter((c) => c.source !== OrganizationSource.UI)

  const toCreate: IMemberEnrichmentDataNormalizedOrganization[] = []
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const toUpdate: Map<IMemberOrganizationData, Record<string, any>> = new Map()

  if (isHighConfidenceSourceSelectedForWorkExperiences) {
    const uiEntries = oldVersion.filter((c) => c.source === OrganizationSource.UI)
    const filteredNewVersion = newVersion.filter(
      (e) =>
        !uiEntries.some(
          (ui) =>
            e.title === ui.jobTitle &&
            e.identities &&
            e.identities.some((i) => i.organizationId === ui.orgId),
        ),
    )
    toCreate.push(...filteredNewVersion)
    return {
      toDelete,
      toCreate,
      toUpdate,
    }
  }

  // sort both versions by start date and only use manual changes from the current version
  const orderedCurrentVersion = oldVersion
    .filter((c) => c.source === OrganizationSource.UI)
    .sort((a, b) => {
      // If either value is null/undefined, move it to the beginning
      if (!a.dateStart && !b.dateStart) return 0
      if (!a.dateStart) return -1
      if (!b.dateStart) return 1

      // Compare dates if both values exist
      return new Date(a.dateStart as string).getTime() - new Date(b.dateStart as string).getTime()
    })

  let orderedNewVersion = newVersion.sort((a, b) => {
    // If either value is null/undefined, move it to the beginning
    if (!a.startDate && !b.startDate) return 0
    if (!a.startDate) return -1
    if (!b.startDate) return 1

    // Compare dates if both values exist
    return new Date(a.startDate as string).getTime() - new Date(b.startDate as string).getTime()
  })

  // set ids and new flag to new versions just so we can easily manipulate the array later
  for (const exp of orderedNewVersion) {
    exp.id = generateUUIDv1()
  }

  // we iterate through the existing version experiences to see if update is needed
  for (const current of orderedCurrentVersion) {
    // try and find a matching experience in the new versions by title
    const match = orderedNewVersion.find(
      (e) =>
        e.title === current.jobTitle &&
        e.identities &&
        e.identities.some((e) => e.organizationId === current.orgId),
    )

    // if we found a match we can check if we need something to update
    if (
      match &&
      current.dateStart === match.startDate &&
      current.dateEnd === null &&
      match.endDate !== null
    ) {
      const toUpdateInner: Record<string, any> = {}

      toUpdateInner.dateEnd = match.endDate
      toUpdate.set(current, toUpdateInner)

      // remove the match from the new version array so we later don't process it again
      orderedNewVersion = orderedNewVersion.filter((e) => e.id !== match.id)
    } else if (
      match &&
      (current.dateStart !== match.startDate || current.dateEnd !== null || match.endDate === null)
    ) {
      // there's an incoming work experiences, but it's conflicting with the existing manually updated data
      // we shouldn't add or update anything when this happens
      // we can only update dateEnd of existing manually changed data, when it has a null dateEnd
      orderedNewVersion = orderedNewVersion.filter((e) => e.id !== match.id)
    }
    // if we didn't find a match we should just leave it as it is in the database since it was manual input
  }

  // the remaining experiences in the new version array are just new experiences to create
  toCreate.push(...orderedNewVersion)

  return {
    toDelete,
    toCreate,
    toUpdate,
  }
}

export async function syncMember(memberId: string): Promise<void> {
  const syncApi = new SearchSyncApiClient({
    baseUrl: process.env['CROWD_SEARCH_SYNC_API_URL'],
  })

  await syncApi.triggerMemberSync(memberId)
}

export async function syncOrganization(organizationId: string): Promise<void> {
  const syncApi = new SearchSyncApiClient({
    baseUrl: process.env['CROWD_SEARCH_SYNC_API_URL'],
  })

  await syncApi.triggerOrganizationSync(organizationId, undefined)
}

export async function cleanAttributeValue(
  attributeValue: string | string[] | Record<string, any>,
): Promise<string | string[] | Record<string, any>> {
  if (!attributeValue) {
    return attributeValue
  }

  if (typeof attributeValue === 'string') {
    return replaceDoubleQuotes(attributeValue)
  }

  if (Array.isArray(attributeValue)) {
    return attributeValue.map((v) => (typeof v === 'string' ? replaceDoubleQuotes(v) : v))
  }

  if (typeof attributeValue === 'object' && attributeValue !== null) {
    const cleanedObject: Record<string, any> = {}
    for (const [key, value] of Object.entries(attributeValue)) {
      if (typeof value === 'string') {
        cleanedObject[key] = replaceDoubleQuotes(value)
      } else {
        cleanedObject[key] = value
      }
    }
    return cleanedObject
  }

  return attributeValue
}
