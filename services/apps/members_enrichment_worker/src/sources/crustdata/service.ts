import axios from 'axios'

import { isDomainExcluded, isValidEmail, replaceDoubleQuotes } from '@crowd/common'
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
  IMemberEnrichmentCrustdataRemainingCredits,
  IMemberEnrichmentDataCrustdata,
} from './types'

const PROFILE_FIELDS = [
  'basic_profile',
  'social_handles',
  'professional_network',
  'experience',
  'education',
  'skills',
]

export default class EnrichmentServiceCrustdata extends LoggerBase implements IEnrichmentService {
  public source: MemberEnrichmentSource = MemberEnrichmentSource.CRUSTDATA
  public platform = `enrichment-${this.source}`

  public alsoFindInputsInSourceCaches: MemberEnrichmentSource[] = [
    MemberEnrichmentSource.PROGAI,
    MemberEnrichmentSource.CLEARBIT,
    MemberEnrichmentSource.SERP,
  ]

  public enrichMembersWithActivityMoreThan = 100

  public enrichableBySql = `("membersGlobalActivityCount".total_count > ${this.enrichMembersWithActivityMoreThan}) AND mi.verified AND ((mi.type = 'username' AND mi.platform = 'linkedin') OR (mi.type = 'email'))`

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

    const hasVerifiedLinkedin = input.linkedin.some((i) => i.value && i.verified)
    const hasWorkEmail = this.getWorkEmails(input).length > 0

    return (
      input.activityCount > this.enrichMembersWithActivityMoreThan &&
      (hasEnrichableLinkedinInCache || hasVerifiedLinkedin || hasWorkEmail)
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

      // Live enrich sends up to 2 verified LinkedIn handles
      // Each match costs 7 credits (realtime) + 1 credit (business email)
      return response.account.credits >= 16
    } catch (error) {
      this.log.error('Error while checking Crustdata account usage', error)
      throw error
    }
  }

  async getData(input: IEnrichmentSourceInput): Promise<IMemberEnrichmentDataCrustdata[] | null> {
    const caches = await findMemberEnrichmentCacheForAllSources(input.memberId)
    const linkedinIdentities = this.findDistinctScrapableLinkedinIdentities(input, caches).slice(
      0,
      2,
    )
    const workEmails = this.getWorkEmails(input)

    let profiles: IMemberEnrichmentDataCrustdata[] = null

    if (linkedinIdentities.length > 0) {
      profiles = await this.getDataUsingLinkedinHandles(linkedinIdentities)
    }

    if (!profiles && workEmails.length > 0) {
      profiles = await this.getDataUsingWorkEmails(workEmails)
    }

    return profiles
  }

  private getWorkEmails(input: IEnrichmentSourceInput): string[] {
    const emails: string[] = []
    const seen = new Set<string>()

    for (const identity of input.emails) {
      if (emails.length >= 5) {
        break
      }

      const email = identity.value?.trim().toLowerCase()
      if (!email || !identity.verified || seen.has(email) || !isValidEmail(email)) {
        continue
      }

      const domain = email.split('@')[1]
      if (!domain || isDomainExcluded(domain)) {
        continue
      }

      seen.add(email)
      emails.push(email)
    }

    return emails
  }

  private async getDataUsingLinkedinHandles(
    identities: ConsumableIdentity[],
  ): Promise<IMemberEnrichmentDataCrustdata[] | null> {
    const urls = identities.map((identity) => `https://www.linkedin.com/in/${identity.value}`)

    const response = await this.requestEnrich('/person/professional_network/enrich/live', {
      professional_network_profile_urls: urls,
      fields: PROFILE_FIELDS,
    })

    const profiles: IMemberEnrichmentDataCrustdata[] = []
    const matchedOns: string[] = []

    for (const result of response || []) {
      const personData = result.matches?.[0]?.person_data
      if (!personData || !result.matched_on) {
        continue
      }

      const handle = result.matched_on.split('/').filter(Boolean).pop()?.toLowerCase()
      const identity = identities.find((i) => i.value.toLowerCase() === handle)
      if (!identity) {
        continue
      }

      matchedOns.push(result.matched_on)
      profiles.push({
        ...personData,
        metadata: {
          repeatedTimesInDifferentSources: identity.repeatedTimesInDifferentSources,
          isFromVerifiedSource: identity.isFromVerifiedSource,
        },
      })
    }

    if (profiles.length === 0) {
      return null
    }

    return this.withContact(profiles, matchedOns, 'linkedin')
  }

  private async getDataUsingWorkEmails(
    emails: string[],
  ): Promise<IMemberEnrichmentDataCrustdata[] | null> {
    const response = await this.requestEnrich('/person/enrich', {
      business_emails: emails,
      fields: PROFILE_FIELDS,
    })

    const profiles: IMemberEnrichmentDataCrustdata[] = []
    const matchedOns: string[] = []

    for (const result of response || []) {
      const personData = result.matches?.[0]?.person_data
      if (!personData || !result.matched_on) {
        continue
      }

      matchedOns.push(result.matched_on)
      profiles.push({
        ...personData,
        metadata: {
          repeatedTimesInDifferentSources: 1,
          isFromVerifiedSource: true,
        },
      })
    }

    if (profiles.length === 0) {
      return null
    }

    return this.withContact(profiles, matchedOns, 'email')
  }

  private async withContact(
    profiles: IMemberEnrichmentDataCrustdata[],
    matchedOns: string[],
    identifierType: 'linkedin' | 'email',
  ): Promise<IMemberEnrichmentDataCrustdata[]> {
    const body =
      identifierType === 'linkedin'
        ? {
            professional_network_profile_urls: matchedOns,
            fields: ['contact.business_emails'],
          }
        : {
            business_emails: matchedOns,
            fields: ['contact.business_emails'],
          }

    let response: IMemberEnrichmentCrustdataEnrichResponse | null

    try {
      // contact is best effort, so we don't want to fail the enrichment if it fails.
      response = await this.requestEnrich('/person/contact/enrich', body)
    } catch (error) {
      if (
        axios.isAxiosError(error) &&
        (error.response?.status === 401 || error.response?.status === 403)
      ) {
        throw error
      }

      this.log.error(
        { source: this.source, error },
        'Crustdata contact enrich failed; keeping profile without contact',
      )
      return profiles
    }

    if (!response?.length) {
      return profiles
    }

    const contactByMatchedOn = new Map(
      response
        .filter((result) => result.matched_on && result.matches?.[0]?.person_data?.contact)
        .map((result) => [
          result.matched_on.trim().toLowerCase().replace(/\/+$/, ''),
          result.matches[0].person_data.contact,
        ]),
    )

    return profiles.map((profile, index) => {
      const contact = contactByMatchedOn.get(
        matchedOns[index].trim().toLowerCase().replace(/\/+$/, ''),
      )
      if (!contact) {
        return profile
      }
      return { ...profile, contact }
    })
  }

  private async requestEnrich(
    path: string,
    data: Record<string, unknown>,
  ): Promise<IMemberEnrichmentCrustdataEnrichResponse | null> {
    const response = await axios<IMemberEnrichmentCrustdataEnrichResponse>({
      method: 'post',
      url: `${process.env['CROWD_ENRICHMENT_CRUSTDATA_URL']}${path}`,
      headers: {
        Authorization: `Bearer ${process.env['CROWD_ENRICHMENT_CRUSTDATA_API_KEY']}`,
        'x-api-version': '2025-11-01',
        'content-type': 'application/json',
      },
      data,
      validateStatus: function (status) {
        return (status >= 200 && status < 300) || status === 404
      },
    })

    if (response.status === 404) {
      return null
    }

    return response.data
  }

  private findDistinctScrapableLinkedinIdentities(
    input: IEnrichmentSourceInput,
    caches: IMemberEnrichmentCache<IMemberEnrichmentData>[],
  ): ConsumableIdentity[] {
    const consumableIdentities: ConsumableIdentity[] = []
    const linkedinUrlHashmap = new Map<string, number>()

    for (const linkedin of input.linkedin) {
      if (linkedin && linkedin.value && linkedin.verified) {
        const handle = linkedin.value.replace(/\//g, '')
        if (!linkedinUrlHashmap.get(handle)) {
          consumableIdentities.push({
            ...linkedin,
            value: handle,
            repeatedTimesInDifferentSources: 1,
            isFromVerifiedSource: true,
          })
          linkedinUrlHashmap.set(handle, 1)
        } else {
          const repeatedTimesInDifferentSources = linkedinUrlHashmap.get(handle) + 1
          linkedinUrlHashmap.set(handle, repeatedTimesInDifferentSources)
          const identityFound = consumableIdentities.find((i) => i.value === handle)
          identityFound.repeatedTimesInDifferentSources = repeatedTimesInDifferentSources
          identityFound.isFromVerifiedSource = true
        }
      }
    }

    for (const cache of caches) {
      if (this.alsoFindInputsInSourceCaches.includes(cache.source)) {
        const service = EnrichmentSourceServiceFactory.getEnrichmentSourceService(
          cache.source,
          this.log,
        )
        const normalized = service.normalize(cache.data) as IMemberEnrichmentDataNormalized
        if (normalized.identities.some((i) => i.platform === PlatformType.LINKEDIN)) {
          const identity = normalized.identities.find((i) => i.platform === PlatformType.LINKEDIN)
          const handle = identity.value.replace(/\//g, '')
          if (!linkedinUrlHashmap.get(handle)) {
            consumableIdentities.push({
              ...identity,
              value: handle,
              repeatedTimesInDifferentSources: 1,
              isFromVerifiedSource: false,
            })
            linkedinUrlHashmap.set(handle, 1)
          } else {
            const repeatedTimesInDifferentSources = linkedinUrlHashmap.get(handle) + 1
            linkedinUrlHashmap.set(handle, repeatedTimesInDifferentSources)
            consumableIdentities.find((i) => i.value === handle).repeatedTimesInDifferentSources =
              repeatedTimesInDifferentSources
          }
        }
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
      const handle = linkedinUrl.split('/').filter(Boolean).pop()
      if (handle) {
        normalized = normalizeSocialIdentity(
          {
            handle,
            platform: PlatformType.LINKEDIN,
          },
          MemberIdentityType.USERNAME,
          normalized,
        )
      }
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

    const seenEmails = new Set<string>()
    for (const entry of data.contact?.business_emails || []) {
      if (entry.status !== 'deliverable' || !entry.email) {
        continue
      }

      const email = entry.email.trim().toLowerCase()
      if (seenEmails.has(email)) {
        continue
      }
      seenEmails.add(email)

      normalized.identities.push({
        value: email,
        type: MemberIdentityType.EMAIL,
        platform: this.platform,
        verified: false,
        source: 'enrichment',
      })
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
