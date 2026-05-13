import axios from 'axios'

import { Logger, LoggerBase } from '@crowd/logging'
import { IMemberEnrichmentCache, MemberEnrichmentSource, PlatformType } from '@crowd/types'

import { findMemberEnrichmentCacheForAllSources } from '../../activities/enrichment'
import { EnrichmentSourceServiceFactory } from '../../factory'
import {
  ConsumableIdentity,
  IEnrichmentService,
  IEnrichmentSourceInput,
  IMemberEnrichmentData,
  IMemberEnrichmentDataNormalized,
} from '../../types'
import { IMemberEnrichmentDataProgAI, IMemberEnrichmentDataProgAIResponse } from '../progai/types'

import { IMemberEnrichmentDataProgAILinkedinScraper } from './types'

export default class EnrichmentServiceProgAILinkedinScraper
  extends LoggerBase
  implements IEnrichmentService
{
  public source: MemberEnrichmentSource = MemberEnrichmentSource.PROGAI_LINKEDIN_SCRAPER
  public platform = `enrichment-${this.source}`

  public alsoFindInputsInSourceCaches: MemberEnrichmentSource[] = [
    MemberEnrichmentSource.PROGAI,
    MemberEnrichmentSource.CLEARBIT,
    MemberEnrichmentSource.SERP,
  ]

  public enrichableBySql = `(mi.verified AND mi.type = 'username' and mi.platform = 'linkedin')`

  public cacheObsoleteAfterSeconds = 60 * 60 * 24 * 90

  public maxConcurrentRequests = 3

  constructor(public readonly log: Logger) {
    super(log)
  }

  // in addition to members with linkedin identity we'll also use existing cache rows from other sources (serp and clearbit)
  async isEnrichableBySource(input: IEnrichmentSourceInput): Promise<boolean> {
    const caches = await findMemberEnrichmentCacheForAllSources(input.memberId)

    let hasEnrichableLinkedinInCache = false
    for (const cache of caches) {
      if (this.alsoFindInputsInSourceCaches.includes(cache.source)) {
        const service = EnrichmentSourceServiceFactory.getEnrichmentSourceService(
          cache.source,
          this.log,
        )
        const normalized = service.normalize(cache.data) as IMemberEnrichmentDataNormalized
        if (normalized.identities.some((i) => i.platform === PlatformType.LINKEDIN)) {
          hasEnrichableLinkedinInCache = true
          break
        }
      }
    }

    return (
      hasEnrichableLinkedinInCache ||
      (input.linkedin && input.linkedin.value && input.linkedin.verified)
    )
  }

  async hasRemainingCredits(): Promise<boolean> {
    return true
  }

  async getData(
    input: IEnrichmentSourceInput,
  ): Promise<IMemberEnrichmentDataProgAILinkedinScraper[] | null> {
    const profiles: IMemberEnrichmentDataProgAILinkedinScraper[] = []
    const caches = await findMemberEnrichmentCacheForAllSources(input.memberId)

    const consumableIdentities = await this.findDistinctScrapableLinkedinIdentities(input, caches)

    for (const identity of consumableIdentities) {
      const data = await this.getDataUsingLinkedinHandle(identity.value)
      if (data) {
        const existingProgaiCache = caches.find((c) => c.source === MemberEnrichmentSource.PROGAI)
        // we don't want to reinforce the cache with the same data, only save to cache
        // if a new profile is returned from progai
        if ((existingProgaiCache?.data as IMemberEnrichmentDataProgAI)?.id == data.id) {
          continue
        }
        profiles.push({
          ...data,
          metadata: {
            repeatedTimesInDifferentSources: identity.repeatedTimesInDifferentSources,
            isFromVerifiedSource: identity.isFromVerifiedSource,
          },
        })
      }
    }

    return profiles.length > 0 ? profiles : null
  }

  private async getDataUsingLinkedinHandle(
    handle: string,
  ): Promise<IMemberEnrichmentDataProgAI | null> {
    const config = {
      method: 'get',
      url: `${process.env['CROWD_ENRICHMENT_PROGAI_URL']}/get_profile`,
      params: {
        linkedin_url: `https://linkedin.com/in/${encodeURIComponent(handle)}`,
        with_emails: true,
        api_key: process.env['CROWD_ENRICHMENT_PROGAI_API_KEY'],
      },
      headers: {},
      validateStatus: function (status) {
        return (status >= 200 && status < 300) || status === 404 || status === 422
      },
    }

    const response = await axios<IMemberEnrichmentDataProgAIResponse>(config)

    if (response.status === 404 || response.status === 422) {
      this.log.debug({ source: this.source, handle }, 'No data found for linkedin handle!')
      return null
    }

    if (!response.data?.profile) {
      return null
    }

    return response.data?.profile
  }

  private async findDistinctScrapableLinkedinIdentities(
    input: IEnrichmentSourceInput,
    caches: IMemberEnrichmentCache<IMemberEnrichmentData>[],
  ): Promise<ConsumableIdentity[]> {
    const consumableIdentities: ConsumableIdentity[] = []
    const linkedinUrlHashmap = new Map<string, number>()

    for (const cache of caches) {
      if (this.alsoFindInputsInSourceCaches.includes(cache.source)) {
        const service = EnrichmentSourceServiceFactory.getEnrichmentSourceService(
          cache.source,
          this.log,
        )
        const normalized = service.normalize(cache.data) as IMemberEnrichmentDataNormalized
        if (normalized.identities.some((i) => i.platform === PlatformType.LINKEDIN)) {
          const identity = normalized.identities.find((i) => i.platform === PlatformType.LINKEDIN)
          if (!linkedinUrlHashmap.get(identity.value)) {
            consumableIdentities.push({
              ...identity,
              repeatedTimesInDifferentSources: 1,
              isFromVerifiedSource: false,
            })
            linkedinUrlHashmap.set(identity.value, 1)
          } else {
            const repeatedTimesInDifferentSources = linkedinUrlHashmap.get(identity.value) + 1
            linkedinUrlHashmap.set(identity.value, repeatedTimesInDifferentSources)
            consumableIdentities.find(
              (i) => i.value === identity.value,
            ).repeatedTimesInDifferentSources = repeatedTimesInDifferentSources
          }
        }
      }
    }

    // also add the linkedin identity from the input
    if (input.linkedin && input.linkedin.value && input.linkedin.verified) {
      if (!linkedinUrlHashmap.get(input.linkedin.value)) {
        consumableIdentities.push({
          ...input.linkedin,
          value: input.linkedin.value.replace(/\//g, ''),
          repeatedTimesInDifferentSources: 1,
          isFromVerifiedSource: true,
        })
      } else {
        const repeatedTimesInDifferentSources = linkedinUrlHashmap.get(input.linkedin.value) + 1
        const identityFound = consumableIdentities.find((i) => i.value === input.linkedin.value)

        identityFound.repeatedTimesInDifferentSources = repeatedTimesInDifferentSources
        identityFound.isFromVerifiedSource = true
      }
    }
    return consumableIdentities
  }

  normalize(
    profiles: IMemberEnrichmentDataProgAILinkedinScraper[],
  ): IMemberEnrichmentDataNormalized[] {
    const normalizedProfiles: IMemberEnrichmentDataNormalized[] = []
    const progaiService = EnrichmentSourceServiceFactory.getEnrichmentSourceService(
      MemberEnrichmentSource.PROGAI,
      this.log,
    )

    for (const profile of profiles) {
      const normalized = progaiService.normalize(profile) as IMemberEnrichmentDataNormalized
      normalizedProfiles.push({ ...normalized, metadata: profile.metadata })
    }

    return normalizedProfiles.length > 0 ? normalizedProfiles : null
  }
}
