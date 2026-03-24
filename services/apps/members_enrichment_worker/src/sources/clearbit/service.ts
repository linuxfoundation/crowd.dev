import axios from 'axios'

import { replaceDoubleQuotes } from '@crowd/common'
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
  IMemberEnrichmentClearbitAPIErrorResponse,
  IMemberEnrichmentClearbitAPIResponse,
  IMemberEnrichmentDataClearbit,
} from './types'

export default class EnrichmentServiceClearbit extends LoggerBase implements IEnrichmentService {
  public source: MemberEnrichmentSource = MemberEnrichmentSource.CLEARBIT
  public platform = `enrichment-${this.source}`
  public enrichMembersWithActivityMoreThan = 100

  public enrichableBySql = `"membersGlobalActivityCount".total_count > ${this.enrichMembersWithActivityMoreThan} AND mi.type = 'email' and mi.verified`

  public maxConcurrentRequests = 15

  // bust cache after 120 days
  public cacheObsoleteAfterSeconds = 60 * 60 * 24 * 120

  public attributeSettings: IMemberEnrichmentAttributeSettings = {
    [MemberAttributeName.LOCATION]: {
      fields: ['location'],
    },
    [MemberAttributeName.TIMEZONE]: {
      fields: ['timezone'],
    },
    [MemberAttributeName.BIO]: {
      fields: ['bio'],
    },
    [MemberAttributeName.WEBSITE_URL]: {
      fields: ['site'],
    },
    [MemberAttributeName.AVATAR_URL]: {
      fields: ['avatar'],
    },
  }

  constructor(public readonly log: Logger) {
    super(log)
  }

  async isEnrichableBySource(input: IEnrichmentSourceInput): Promise<boolean> {
    return (
      input.activityCount > this.enrichMembersWithActivityMoreThan &&
      !!input.email?.value &&
      input.email?.verified
    )
  }

  async getData(input: IEnrichmentSourceInput): Promise<IMemberEnrichmentDataClearbit | null> {
    const enriched: IMemberEnrichmentDataClearbit = await this.getDataUsingEmail(input.email.value)
    return enriched
  }

  async hasRemainingCredits(): Promise<boolean> {
    return true
  }

  private async getDataUsingEmail(email: string): Promise<IMemberEnrichmentDataClearbit | null> {
    const config = {
      method: 'get',
      url: `${process.env['CROWD_ENRICHMENT_CLEARBIT_URL']}`,
      params: {
        email,
      },
      headers: {
        Authorization: `Bearer ${process.env['CROWD_ENRICHMENT_CLEARBIT_API_KEY']}`,
      },
      validateStatus: function (status) {
        return (status >= 200 && status < 300) || status === 404 || status === 422
      },
    }

    const response = await axios(config)

    if (response.status === 404 || response.status === 422) {
      this.log.debug({ source: this.source, email }, 'No data found for email!')
      return null
    }

    if (this.isErrorResponse(response.data)) {
      return null
    }

    return response.data
  }

  private isErrorResponse(
    response: IMemberEnrichmentClearbitAPIResponse,
  ): response is IMemberEnrichmentClearbitAPIErrorResponse {
    return (response as IMemberEnrichmentClearbitAPIErrorResponse).error !== undefined
  }

  normalize(data: IMemberEnrichmentDataClearbit): IMemberEnrichmentDataNormalized {
    let normalized: IMemberEnrichmentDataNormalized = {
      identities: [],
      attributes: {},
      memberOrganizations: [],
    }

    if (data.name?.fullName) {
      normalized.displayName = data.name.fullName
    }

    normalized = this.normalizeIdentities(data, normalized)
    normalized = normalizeAttributes(data, normalized, this.attributeSettings, this.platform)
    normalized = this.normalizeEmployment(data, normalized)

    return normalized
  }

  private normalizeIdentities(
    data: IMemberEnrichmentDataClearbit,
    normalized: IMemberEnrichmentDataNormalized,
  ): IMemberEnrichmentDataNormalized {
    if (data.email) {
      normalized.identities.push({
        value: data.email,
        type: MemberIdentityType.EMAIL,
        platform: this.platform,
        verified: false,
        source: 'enrichment',
      })
    }

    if (data.facebook?.handle) {
      normalized = normalizeSocialIdentity(
        {
          handle: data.facebook.handle,
          platform: PlatformType.FACEBOOK,
        },
        MemberIdentityType.USERNAME,
        normalized,
      )
    }

    if (data.github?.handle) {
      normalized = normalizeSocialIdentity(
        {
          handle: data.github.handle,
          platform: PlatformType.GITHUB,
        },
        MemberIdentityType.USERNAME,
        normalized,
      )
    }

    if (data.linkedin?.handle) {
      normalized = normalizeSocialIdentity(
        {
          handle: data.linkedin.handle.endsWith('/')
            ? data.linkedin.handle.slice(0, -1).split('/').pop()
            : data.linkedin.handle.split('/').pop(),
          platform: PlatformType.LINKEDIN,
        },
        MemberIdentityType.USERNAME,
        normalized,
      )
    }

    if (data.twitter?.handle) {
      normalized = normalizeSocialIdentity(
        {
          handle: data.twitter.handle,
          platform: PlatformType.TWITTER,
        },
        MemberIdentityType.USERNAME,
        normalized,
      )
    }

    return normalized
  }

  private normalizeEmployment(
    data: IMemberEnrichmentDataClearbit,
    normalized: IMemberEnrichmentDataNormalized,
  ): IMemberEnrichmentDataNormalized {
    if (data.employment?.name) {
      const orgIdentities = []
      if (data.employment?.domain) {
        orgIdentities.push({
          platform: this.platform,
          value: data.employment.domain,
          type: OrganizationIdentityType.PRIMARY_DOMAIN,
          verified: true,
          source: 'enrichment',
        })
      }

      normalized.memberOrganizations.push({
        name: replaceDoubleQuotes(data.employment.name),
        source: OrganizationSource.ENRICHMENT_CLEARBIT,
        identities: orgIdentities,
        title: replaceDoubleQuotes(data.employment.title),
        startDate: null,
        endDate: null,
      })
    }
    return normalized
  }
}
