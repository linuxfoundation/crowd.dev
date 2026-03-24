import axios from 'axios'
import lodash from 'lodash'

import { normalizeHostname, replaceDoubleQuotes } from '@crowd/common'
import { Logger, LoggerBase } from '@crowd/logging'
import {
  MemberAttributeName,
  MemberEnrichmentSource,
  MemberIdentityType,
  OrganizationIdentityType,
  OrganizationSource,
  PlatformType,
} from '@crowd/types'

import {
  IEnrichmentService,
  IEnrichmentSourceInput,
  IMemberEnrichmentAttributeSettings,
  IMemberEnrichmentDataNormalized,
} from '../../types'
import { normalizeAttributes, normalizeSocialIdentity } from '../../utils/common'

import {
  IEnrichmentAPICertificationProgAI,
  IEnrichmentAPIContributionProgAI,
  IEnrichmentAPIEducationProgAI,
  IEnrichmentAPISkillsProgAI,
  IMemberEnrichmentDataProgAI,
  IMemberEnrichmentDataProgAIResponse,
} from './types'

export default class EnrichmentServiceProgAI extends LoggerBase implements IEnrichmentService {
  public source: MemberEnrichmentSource = MemberEnrichmentSource.PROGAI
  public platform = `enrichment-${this.source}`

  public enrichableBySql = `mi.verified and ((mi.type = 'username' AND mi.platform = 'github') OR (mi.type = 'email'))`

  // bust cache after 90 days
  public cacheObsoleteAfterSeconds = 60 * 60 * 24 * 90

  public maxConcurrentRequests = 3

  public attributeSettings: IMemberEnrichmentAttributeSettings = {
    [MemberAttributeName.AVATAR_URL]: {
      fields: ['profile_pic_url'],
    },
    [MemberAttributeName.LOCATION]: {
      fields: ['location'],
    },
    [MemberAttributeName.BIO]: {
      fields: ['title', 'work_experiences[0].title'],
    },
    [MemberAttributeName.SENIORITY_LEVEL]: {
      fields: ['seniority_level'],
    },
    [MemberAttributeName.COUNTRY]: {
      fields: ['country'],
    },
    [MemberAttributeName.PROGRAMMING_LANGUAGES]: {
      fields: ['programming_languages'],
      transform: (programmingLanguages: string[]) => programmingLanguages.sort(),
    },
    [MemberAttributeName.LANGUAGES]: {
      fields: ['languages'],
      transform: (languages: string[]) => languages.sort(),
    },
    [MemberAttributeName.YEARS_OF_EXPERIENCE]: {
      fields: ['years_of_experience'],
    },
    [MemberAttributeName.EXPERTISE]: {
      fields: ['expertise'],
    },
    [MemberAttributeName.EDUCATION]: {
      fields: ['educations'],
      transform: (educations: IEnrichmentAPIEducationProgAI[]) =>
        educations.map((education) => {
          const { campus, major, specialization, startDate, endDate } = education
          return {
            campus,
            major,
            specialization,
            startDate,
            endDate: endDate || 'Present',
          }
        }),
    },
    [MemberAttributeName.AWARDS]: {
      fields: ['awards'],
    },
    [MemberAttributeName.CERTIFICATIONS]: {
      fields: ['certifications'],
      transform: (certifications: IEnrichmentAPICertificationProgAI[]) =>
        certifications.map((certification) => {
          const { title, description } = certification
          return {
            title,
            description,
          }
        }),
    },
  }

  constructor(
    public readonly log: Logger,
    private readonly alsoUseEmailIdentitiesForEnrichment: boolean,
    private readonly enrichEmailIdentities: boolean,
  ) {
    super(log)
  }

  async isEnrichableBySource(input: IEnrichmentSourceInput): Promise<boolean> {
    const enrichableUsingGithubHandle = !!input.github?.value
    const enrichableUsingEmail = this.alsoUseEmailIdentitiesForEnrichment && !!input.email?.value
    return enrichableUsingGithubHandle || enrichableUsingEmail
  }

  async hasRemainingCredits(): Promise<boolean> {
    return true
  }

  async getData(input: IEnrichmentSourceInput): Promise<IMemberEnrichmentDataProgAI> {
    let enriched: IMemberEnrichmentDataProgAI = null

    // get data logic
    if (input.github) {
      enriched = await this.getDataUsingGitHubHandle(input.github.value)
    }

    if (this.alsoUseEmailIdentitiesForEnrichment) {
      if (!enriched && input.email) {
        enriched = await this.getDataUsingEmailAddress(input.email.value)
      }
    }

    return enriched
  }

  normalize(data: IMemberEnrichmentDataProgAI): IMemberEnrichmentDataNormalized {
    let normalized: IMemberEnrichmentDataNormalized = {
      identities: [],
      attributes: {},
      contributions: [],
      memberOrganizations: [],
    }
    normalized = this.fillPlatformData(data, normalized)
    normalized = normalizeAttributes(data, normalized, this.attributeSettings, this.platform)
    normalized = this.fillSkills(data, normalized)
    normalized = this.normalizeEmployment(data, normalized)

    return normalized
  }

  private fillSkills(
    data: IMemberEnrichmentDataProgAI,
    normalized: IMemberEnrichmentDataNormalized,
  ): IMemberEnrichmentDataNormalized {
    if (!normalized.attributes) {
      normalized.attributes = {}
    }

    if (data.skills) {
      if (!normalized.attributes.skills) {
        normalized.attributes.skills = {}
      }

      // Assign unique and ordered skills to 'member.attributes[MemberAttributeName.SKILLS].enrichment'
      normalized.attributes[MemberAttributeName.SKILLS].enrichment = lodash.uniq([
        // Use 'lodash.orderBy' to sort the skills by weight in descending order
        ...lodash
          .orderBy(data.skills || [], ['weight'], ['desc'])
          .map((s: IEnrichmentAPISkillsProgAI) => s.skill),
      ])
    }

    return normalized
  }

  private fillPlatformData(
    data: IMemberEnrichmentDataProgAI,
    normalized: IMemberEnrichmentDataNormalized,
  ): IMemberEnrichmentDataNormalized {
    if (!normalized.identities) {
      normalized.identities = []
    }

    if (!normalized.attributes) {
      normalized.attributes = {}
    }

    if (this.enrichEmailIdentities) {
      if (data.emails && Array.isArray(data.emails)) {
        if (data.emails.length > 0) {
          const emailSet = new Set<string>(
            data.emails.filter((email) => !email.includes('noreply.github')),
          )

          for (const email of emailSet) {
            normalized.identities.push({
              value: email,
              type: MemberIdentityType.EMAIL,
              platform: this.platform,
              verified: false,
              source: 'enrichment',
            })
          }
        }
      }
    }

    normalized.contributions = data.oss_contributions?.map(
      (contribution: IEnrichmentAPIContributionProgAI) => ({
        id: contribution.id,
        topics: contribution.topics,
        summary: contribution.summary,
        url: contribution.github_url,
        firstCommitDate: contribution.first_commit_date,
        lastCommitDate: contribution.last_commit_date,
        numberCommits: contribution.num_of_commits,
      }),
    )

    if (data.github_handle) {
      normalized = normalizeSocialIdentity(
        {
          handle: data.github_handle,
          platform: PlatformType.GITHUB,
        },
        MemberIdentityType.USERNAME,
        normalized,
      )
    }

    if (data.linkedin_url) {
      const linkedinHandle = data.linkedin_url.split('/').pop()
      normalized = normalizeSocialIdentity(
        {
          handle: linkedinHandle,
          platform: PlatformType.LINKEDIN,
        },
        MemberIdentityType.USERNAME,
        normalized,
      )
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

    return normalized
  }

  private normalizeEmployment(
    data: IMemberEnrichmentDataProgAI,
    normalized: IMemberEnrichmentDataNormalized,
  ): IMemberEnrichmentDataNormalized {
    if (data.work_experiences) {
      for (const workExperience of data.work_experiences) {
        if (
          workExperience.company !== null ||
          workExperience.companyUrl !== null ||
          workExperience.companyLinkedInUrl !== null
        ) {
          const identities = []
          let hasPrimaryDomainIdentity = false

          if (workExperience.companyUrl) {
            const normalizedDomain = normalizeHostname(workExperience.companyUrl, false)

            // sometimes companyUrl is a github link, we don't want to add it as a primary domain
            if (
              normalizedDomain &&
              !workExperience.companyUrl.toLowerCase().includes('github') &&
              !(workExperience.company || '').toLowerCase().includes('github')
            ) {
              identities.push({
                platform: PlatformType.LINKEDIN,
                value: normalizedDomain,
                type: OrganizationIdentityType.PRIMARY_DOMAIN,
                verified: true,
              })
              hasPrimaryDomainIdentity = true
            }
          }

          if (
            workExperience.companyLinkedInUrl &&
            this.getLinkedInProfileHandle(workExperience.companyLinkedInUrl)
          ) {
            identities.push({
              platform: PlatformType.LINKEDIN,
              value: this.getLinkedInProfileHandle(workExperience.companyLinkedInUrl),
              type: OrganizationIdentityType.USERNAME,
              verified: !hasPrimaryDomainIdentity,
            })
          }

          // ProgAI uses epoch (1970-01-01) to represent "unknown" or "ongoing" dates.
          // We treat these as null since they break timeline logic downstream.
          const startDate = this.normalizeDate(workExperience.startDate)
          const endDate = this.normalizeDate(workExperience.endDate)

          normalized.memberOrganizations.push({
            name: replaceDoubleQuotes(workExperience.company),
            source: OrganizationSource.ENRICHMENT_PROGAI,
            identities,
            title: replaceDoubleQuotes(workExperience.title),
            startDate,
            endDate,
          })
        }
      }
    }

    return normalized
  }

  /**
   * Normalizes a date string from ProgAI.
   * Returns null if the date is missing or is the Unix epoch (1970-01-01),
   * which ProgAI uses to represent "unknown" or "ongoing" dates.
   */
  private normalizeDate(date: string | null | undefined): string | null {
    if (!date) return null

    if (date.startsWith('1970-01-01')) return null

    return date.replace('Z', '+00:00')
  }

  private getLinkedInProfileHandle(url: string): string | null {
    let regex = /company\/([^/]+)/
    let match = url.match(regex)

    if (match) {
      return `company:${match[1]}`
    }

    regex = /school\/([^/]+)/
    match = url.match(regex)

    if (match) {
      return `school:${match[1]}`
    }

    return null
  }

  async getDataUsingGitHubHandle(
    githubUsername: string,
  ): Promise<IMemberEnrichmentDataProgAI | null> {
    const config = {
      method: 'get',
      url: `${process.env['CROWD_ENRICHMENT_PROGAI_URL']}/get_profile`,
      params: {
        github_handle: githubUsername,
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
      this.log.debug({ source: this.source, githubUsername }, 'No data found for github handle!')
      return null
    }

    if (!response.data?.profile) {
      return null
    }

    return response.data?.profile
  }

  async getDataUsingEmailAddress(email: string): Promise<IMemberEnrichmentDataProgAI | null> {
    const config = {
      method: 'get',
      url: `${process.env['CROWD_ENRICHMENT_PROGAI_URL']}/get_profile`,
      params: {
        email,
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
      this.log.debug({ source: this.source, email }, 'No data found for email!')
      return null
    }

    if (!response.data?.profile) {
      return null
    }

    return response.data?.profile
  }
}
