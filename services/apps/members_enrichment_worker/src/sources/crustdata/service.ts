import axios from 'axios'

import { isEmail, replaceDoubleQuotes } from '@crowd/common'
import { Logger, LoggerBase } from '@crowd/logging'
import {
  IMemberEnrichmentCache,
  MemberAttributeName,
  MemberEnrichmentSource,
  MemberIdentityType,
  OrganizationIdentityType,
  OrganizationSource,
  PlatformType,
} from '@crowd/types'

import { findMemberEnrichmentCacheForAllSources } from '../../activities/enrichment'
import { EnrichmentSourceServiceFactory } from '../../factory'
import {
  ConsumableIdentity,
  IEnrichmentService,
  IEnrichmentSourceInput,
  IMemberEnrichmentAttributeSettings,
  IMemberEnrichmentData,
  IMemberEnrichmentDataNormalized,
} from '../../types'
import { normalizeAttributes, normalizeSocialIdentity } from '../../utils/common'

import {
  IMemberEnrichmentCrustdataAPIErrorResponse,
  IMemberEnrichmentCrustdataAPIResponse,
  IMemberEnrichmentCrustdataRemainingCredits,
  IMemberEnrichmentDataCrustdata,
} from './types'

export default class EnrichmentServiceCrustdata extends LoggerBase implements IEnrichmentService {
  public source: MemberEnrichmentSource = MemberEnrichmentSource.CRUSTDATA
  public platform = `enrichment-${this.source}`

  public alsoFindInputsInSourceCaches: MemberEnrichmentSource[] = [
    MemberEnrichmentSource.PROGAI,
    MemberEnrichmentSource.CLEARBIT,
    MemberEnrichmentSource.SERP,
  ]

  public enrichMembersWithActivityMoreThan = 100

  public enrichableBySql = `("membersGlobalActivityCount".total_count > ${this.enrichMembersWithActivityMoreThan}) AND mi.verified AND mi.type = 'username' and mi.platform = 'linkedin'`

  public neverReenrich = true

  public maxConcurrentRequests = 5

  public attributeSettings: IMemberEnrichmentAttributeSettings = {
    [MemberAttributeName.AVATAR_URL]: {
      fields: ['profile_picture_url'],
    },
    [MemberAttributeName.JOB_TITLE]: {
      fields: ['title'],
    },
    [MemberAttributeName.BIO]: {
      fields: ['summary', 'headline'],
    },
    [MemberAttributeName.SKILLS]: {
      fields: ['skills'],
      // Note: Crustdata API docs specify skills as string, but API returns string[]
      // So we're handling both cases in the transformer.
      transform: (skills: string | string[]) => {
        if (!skills) {
          return []
        }

        const arr = Array.isArray(skills) ? skills : skills.split(',')

        return arr
          .map((s) => s.trim())
          .filter(Boolean)
          .sort()
      },
    },
    [MemberAttributeName.LANGUAGES]: {
      fields: ['languages'],
      transform: (languages: string[]) => languages.sort(),
    },
    [MemberAttributeName.SCHOOLS]: {
      fields: ['all_schools'],
      transform: (schools: string[]) => schools.sort(),
    },
  }

  constructor(public readonly log: Logger) {
    super(log)
  }

  async isEnrichableBySource(input: IEnrichmentSourceInput): Promise<boolean> {
    // Include cache rows with null data so we can detect members where
    // Crustdata was already attempted but returned no results.
    const caches = await findMemberEnrichmentCacheForAllSources(input.memberId, true)

    // Skip if Crustdata has already been tried for this member.
    const hasCrustdataCache = caches.some((cache) => cache.source === this.source)
    if (hasCrustdataCache) {
      this.log.debug(
        { memberId: input.memberId },
        'Skipping Crustdata for previously enriched profile!',
      )

      return false
    }

    // Check other sources' caches for a LinkedIn identity to scrape.
    const cachesWithData = caches.filter((cache) => cache.data !== null)
    let hasEnrichableLinkedinInCache = false
    for (const cache of cachesWithData) {
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
      input.activityCount > this.enrichMembersWithActivityMoreThan &&
      (hasEnrichableLinkedinInCache ||
        (input.linkedin && input.linkedin.value && input.linkedin.verified))
    )
  }

  async hasRemainingCredits(): Promise<boolean> {
    try {
      const config = {
        method: 'get',
        url: `${process.env['CROWD_ENRICHMENT_CRUSTDATA_URL']}/user/credits`,
        headers: {
          Authorization: `Token ${process.env['CROWD_ENRICHMENT_CRUSTDATA_API_KEY']}`,
        },
      }

      const response: IMemberEnrichmentCrustdataRemainingCredits = (await axios(config)).data

      // realtime linkedin enrichment costs 5 credits
      return response.credits > 5
    } catch (error) {
      this.log.error('Error while checking Crustdata account usage', error)
      return false
    }
  }

  async getData(input: IEnrichmentSourceInput): Promise<IMemberEnrichmentDataCrustdata[] | null> {
    const profiles: IMemberEnrichmentDataCrustdata[] = []
    const caches = await findMemberEnrichmentCacheForAllSources(input.memberId)

    const consumableIdentities = await this.findDistinctScrapableLinkedinIdentities(input, caches)

    for (const identity of consumableIdentities) {
      const data = await this.getDataUsingLinkedinHandle(identity.value)
      if (data) {
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
  ): Promise<IMemberEnrichmentDataCrustdata | null> {
    const config = {
      method: 'get',
      url: `${process.env['CROWD_ENRICHMENT_CRUSTDATA_URL']}/screener/person/enrich`,
      params: {
        linkedin_profile_url: `https://linkedin.com/in/${encodeURIComponent(handle)}`,
        enrich_realtime: true,
      },
      headers: {
        Authorization: `Token ${process.env['CROWD_ENRICHMENT_CRUSTDATA_API_KEY']}`,
      },
      validateStatus: function (status) {
        return (status >= 200 && status < 300) || status === 404 || status === 422
      },
    }

    const response = await axios(config)

    if (response.status === 404 || response.status === 422) {
      this.log.debug({ source: this.source, handle }, 'No data found for linkedin handle!')
      return null
    }

    if (response.data.length === 0 || this.isErrorResponse(response.data[0])) {
      return null
    }

    return response.data[0]
  }

  private isErrorResponse(
    response: IMemberEnrichmentCrustdataAPIResponse,
  ): response is IMemberEnrichmentCrustdataAPIErrorResponse {
    return (response as IMemberEnrichmentCrustdataAPIErrorResponse).error !== undefined
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

  normalize(profiles: IMemberEnrichmentDataCrustdata[]): IMemberEnrichmentDataNormalized[] {
    const normalizedProfiles: IMemberEnrichmentDataNormalized[] = []

    for (const profile of profiles) {
      const profileNormalized = this.normalizeOneResult(profile)
      normalizedProfiles.push({ ...profileNormalized, metadata: profile.metadata })
    }

    return normalizedProfiles.length > 0 ? normalizedProfiles : null
  }

  private normalizeOneResult(
    data: IMemberEnrichmentDataCrustdata,
  ): IMemberEnrichmentDataNormalized {
    let normalized: IMemberEnrichmentDataNormalized = {
      identities: [],
      attributes: {},
      memberOrganizations: [],
      reach: {},
    }

    normalized = this.normalizeIdentities(data, normalized)
    normalized = normalizeAttributes(data, normalized, this.attributeSettings, this.platform)
    normalized = this.normalizeEmployment(data, normalized)

    if (data.num_of_connections) {
      normalized.reach[this.platform] = data.num_of_connections
    }

    return normalized
  }

  private normalizeIdentities(
    data: IMemberEnrichmentDataCrustdata,
    normalized: IMemberEnrichmentDataNormalized,
  ): IMemberEnrichmentDataNormalized {
    if (!normalized.identities) {
      normalized.identities = []
    }

    if (!normalized.attributes) {
      normalized.attributes = {}
    }

    if (data.name) {
      normalized.displayName = data.name
    }

    if (data.email) {
      let emails: string[]

      if (Array.isArray(data.email)) {
        emails = data.email
      } else {
        emails = data.email.split(',').filter(isEmail)
      }

      for (const email of emails) {
        normalized.identities.push({
          type: MemberIdentityType.EMAIL,
          platform: this.platform,
          value: email.trim(),
          verified: false,
          source: 'enrichment',
        })
      }
    }

    if (data.twitter_handle) {
      normalized = normalizeSocialIdentity(
        {
          handle: data.twitter_handle,
          platform: PlatformType.TWITTER,
        },
        MemberIdentityType.USERNAME,
        normalized,
      )
    }

    if (data.linkedin_flagship_url) {
      normalized = normalizeSocialIdentity(
        {
          handle: data.linkedin_flagship_url.split('/').pop(),
          platform: PlatformType.LINKEDIN,
        },
        MemberIdentityType.USERNAME,
        normalized,
      )
    }

    return normalized
  }

  private normalizeEmployment(
    data: IMemberEnrichmentDataCrustdata,
    normalized: IMemberEnrichmentDataNormalized,
  ): IMemberEnrichmentDataNormalized {
    if (!normalized.memberOrganizations) {
      normalized.memberOrganizations = []
    }

    const employmentInformation = (data.past_employers || []).concat(data.current_employers || [])
    if (employmentInformation.length > 0) {
      for (const workExperience of employmentInformation) {
        const identities = []

        if (workExperience.employer_linkedin_id) {
          identities.push({
            platform: PlatformType.LINKEDIN,
            value: `company:${workExperience.employer_linkedin_id}`,
            type: OrganizationIdentityType.USERNAME,
            verified: true,
            source: 'enrichment',
          })
        }

        normalized.memberOrganizations.push({
          name: replaceDoubleQuotes(workExperience.employer_name),
          source: OrganizationSource.ENRICHMENT_CRUSTDATA,
          identities,
          title: replaceDoubleQuotes(workExperience.employee_title),
          startDate: workExperience?.start_date ?? null,
          endDate: workExperience?.end_date ?? null,
          organizationDescription: replaceDoubleQuotes(
            workExperience.employer_linkedin_description,
          ),
        })
      }
    }

    return normalized
  }
}
