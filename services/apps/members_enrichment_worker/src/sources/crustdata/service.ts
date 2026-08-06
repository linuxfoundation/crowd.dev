import axios from 'axios'

import { replaceDoubleQuotes } from '@crowd/common'
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
  IMemberEnrichmentCrustdataEnrichResponse,
  IMemberEnrichmentCrustdataPersonData,
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
      // Fallback order: stable permalink, then CDN url.
      fields: [
        'professional_network.profile_picture_permalink',
        'basic_profile.profile_picture_permalink',
        'professional_network.profile_picture_url',
      ],
    },
    [MemberAttributeName.JOB_TITLE]: {
      fields: ['basic_profile.current_title'],
    },
    [MemberAttributeName.BIO]: {
      fields: ['basic_profile.summary', 'basic_profile.headline'],
    },
    [MemberAttributeName.SKILLS]: {
      fields: ['skills.professional_network_skills'],
      transform: (skills: string[]) => {
        if (!skills) {
          return []
        }

        return skills
          .map((s) => s.trim())
          .filter(Boolean)
          .sort()
      },
    },
    [MemberAttributeName.LANGUAGES]: {
      fields: ['basic_profile.languages'],
      transform: (languages: string[]) => (languages || []).sort(),
    },
    [MemberAttributeName.SCHOOLS]: {
      // education.schools[] is one entry per degree; CDP stores school names only.
      fields: ['education.schools'],
      transform: (schools: Array<{ school?: string }>) =>
        [...new Set((schools || []).map((s) => s.school?.trim()).filter(Boolean))].sort(),
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
        url: `${process.env['CROWD_ENRICHMENT_CRUSTDATA_URL']}/account/credits`,
        headers: {
          Authorization: `Bearer ${process.env['CROWD_ENRICHMENT_CRUSTDATA_API_KEY']}`,
          'x-api-version': '2025-11-01',
        },
      }

      const response: IMemberEnrichmentCrustdataRemainingCredits = (await axios(config)).data

      // Live enrich costs 7 credits per profile.
      return response.account.credits >= 7
    } catch (error) {
      this.log.error('Error while checking Crustdata account usage', error)
      throw error
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
  ): Promise<IMemberEnrichmentCrustdataPersonData | null> {
    const config = {
      method: 'post',
      url: `${process.env['CROWD_ENRICHMENT_CRUSTDATA_URL']}/person/professional_network/enrich/live`,
      headers: {
        Authorization: `Bearer ${process.env['CROWD_ENRICHMENT_CRUSTDATA_API_KEY']}`,
        'x-api-version': '2025-11-01',
        'content-type': 'application/json',
      },
      data: {
        professional_network_profile_urls: [`https://www.linkedin.com/in/${handle}`],
        // Default response is only basic_profile + social_handles.
        fields: [
          'basic_profile',
          'social_handles',
          'professional_network',
          'experience',
          'education',
          'skills',
        ],
      },
      validateStatus: function (status) {
        return (status >= 200 && status < 300) || status === 404
      },
    }

    const response = await axios<IMemberEnrichmentCrustdataEnrichResponse>(config)

    if (response.status === 404) {
      this.log.debug({ source: this.source, handle }, 'No data found for linkedin handle!')
      return null
    }

    // No match returns 200 with empty matches[].
    const match = response.data?.[0]?.matches?.[0]
    if (!match?.person_data) {
      this.log.debug({ source: this.source, handle }, 'No data found for linkedin handle!')
      return null
    }

    return match.person_data
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

    if (data.professional_network?.connections) {
      normalized.reach[this.platform] = data.professional_network.connections
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

    if (data.basic_profile?.name) {
      normalized.displayName = data.basic_profile.name
    }

    // Crustdata social_handles use generic identifiers:
    // professional_network = LinkedIn, dev_platform = GitHub.
    const twitterHandle = data.social_handles?.twitter_identifier?.slug
    if (twitterHandle) {
      normalized = normalizeSocialIdentity(
        {
          handle: twitterHandle,
          platform: PlatformType.TWITTER,
        },
        MemberIdentityType.USERNAME,
        normalized,
      )
    }

    const linkedinUrl = data.social_handles?.professional_network_identifier?.profile_url
    if (linkedinUrl) {
      normalized = normalizeSocialIdentity(
        {
          handle: linkedinUrl.split('/').filter(Boolean).pop(),
          platform: PlatformType.LINKEDIN,
        },
        MemberIdentityType.USERNAME,
        normalized,
      )
    }

    const githubUrl = data.social_handles?.dev_platform_identifier?.profile_url
    if (githubUrl) {
      const handle = githubUrl.split('/').filter(Boolean).pop()
      if (handle) {
        normalized = normalizeSocialIdentity(
          {
            handle,
            platform: PlatformType.GITHUB,
          },
          MemberIdentityType.USERNAME,
          normalized,
        )
      }
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

    const employmentInformation = (data.experience?.employment_details?.past || []).concat(
      data.experience?.employment_details?.current || [],
    )
    if (employmentInformation.length > 0) {
      for (const workExperience of employmentInformation) {
        const identities = []

        if (workExperience.professional_network_id) {
          identities.push({
            platform: PlatformType.LINKEDIN,
            value: `company:${workExperience.professional_network_id}`,
            type: OrganizationIdentityType.USERNAME,
            verified: true,
            source: 'enrichment',
          })
        }

        normalized.memberOrganizations.push({
          name: replaceDoubleQuotes(workExperience.name),
          source: OrganizationSource.ENRICHMENT_CRUSTDATA,
          identities,
          title: replaceDoubleQuotes(workExperience.title),
          startDate: workExperience?.start_date ?? null,
          endDate: workExperience?.end_date ?? null,
        })
      }
    }

    return normalized
  }
}
