import axios from 'axios'

import { cleanURL } from '@crowd/common'
import { Logger, LoggerBase } from '@crowd/logging'
import {
  IOrganizationIdentity,
  OrganizationEnrichmentSource,
  OrganizationIdentityType,
  PlatformType,
} from '@crowd/types'

import {
  IOrganizationEnrichmentData,
  IOrganizationEnrichmentDataNormalized,
  IOrganizationEnrichmentService,
  IOrganizationEnrichmentSourceInput,
} from '../../types'
import {
  IOrganizationEnrichmentDataInternalAPI,
  IOrganizationEnrichmentDataInternalAPIResponse,
} from './types'

export default class EnrichmentServiceLFXInternalAPI
  extends LoggerBase
  implements IOrganizationEnrichmentService
{
  public source: OrganizationEnrichmentSource = OrganizationEnrichmentSource.LFX_INTERNAL_API
  public platform = `enrichment-${this.source}`
  public enrichOrganizationsWithActivityMoreThan = 10

  public enrichableBySql = `"organizationsGlobalActivityCount".total_count_estimate > ${this.enrichOrganizationsWithActivityMoreThan}`

  public maxConcurrentRequests = 4

  public cacheObsoleteAfterSeconds = 60 * 60 * 24 * 90

  constructor(public readonly log: Logger) {
    super(log)
  }

  async isEnrichableBySource(input: IOrganizationEnrichmentSourceInput): Promise<boolean> {
    return input.activityCount > this.enrichOrganizationsWithActivityMoreThan
  }

  async hasRemainingCredits(): Promise<boolean> {
    return true
  }

  async getData(
    input: IOrganizationEnrichmentSourceInput,
  ): Promise<IOrganizationEnrichmentDataInternalAPI | null> {
    // API expects x-www-form-urlencoded payload
    // so we construct a URLSearchParams object and append the data.
    const params = new URLSearchParams()
    params.append(
      'message',
      JSON.stringify({
        name: input.displayName,
        urls: input.domains.map((domain) => cleanURL(domain.value)),
      }),
    )
    params.append('stream', 'false')
    params.append('user_id', 'cdp-organization-enrichment')

    const config = {
      method: 'post',
      url: process.env['CROWD_ORGANIZATION_ENRICHMENT_INTERNAL_API_URL'],
      headers: {
        Authorization: `Bearer ${process.env['CROWD_ORGANIZATION_ENRICHMENT_INTERNAL_API_KEY']}`,
        'Content-Type': 'application/x-www-form-urlencoded',
      },
      data: params.toString(),
      validateStatus: function (status: number) {
        return (status >= 200 && status < 300) || status === 404 || status === 422
      },
    }

    const response = await axios<IOrganizationEnrichmentDataInternalAPIResponse>(config)

    if (response.status === 404 || response.status === 422) {
      this.log.debug(
        { source: this.source, displayName: input.displayName, domains: input.domains },
        'No data found for organization enrichment!',
      )

      return null
    }

    if (!response.data?.content?.profile) {
      return null
    }

    return response.data?.content?.profile
  }

  normalize(data: IOrganizationEnrichmentData): IOrganizationEnrichmentDataNormalized {
    const identities: IOrganizationIdentity[] = []

    if (data.primary_domain) {
      identities.push({
        type: OrganizationIdentityType.PRIMARY_DOMAIN,
        platform: this.platform,
        value: data.primary_domain,
        verified: false,
      })
    }

    if (data.domains && Array.isArray(data.domains)) {
      for (const domain of data.domains) {
        if (domain !== data.primary_domain) {
          identities.push({
            type: OrganizationIdentityType.ALTERNATIVE_DOMAIN,
            platform: this.platform,
            value: domain,
            verified: false,
          })
        }
      }
    }

    if (data.identities) {
      identities.push(
        ...Object.entries(data.identities).flatMap(([key, values]) => {
          const platform = PlatformType[key.toUpperCase() as keyof typeof PlatformType]
          if (!platform) {
            this.log.warn({ key }, 'Unknown platform in identities, skipping')
            return []
          }

          return (Array.isArray(values) ? values : [values])
            .filter((v) => v && typeof v === 'string')
            .map((rawValue) => {
              let value = rawValue

              // match identity values to existing data patterns
              if (platform === PlatformType.CRUNCHBASE) {
                value = rawValue.replace(/^organization\//, '')
              } else if (platform === PlatformType.LINKEDIN) {
                value = rawValue.replace(/^company\//, 'company:')
              }

              return {
                type: OrganizationIdentityType.USERNAME,
                platform,
                value,
                verified: false,
              }
            })
        }),
      )
    }

    if (data.emails && Array.isArray(data.emails)) {
      for (const email of data.emails) {
        identities.push({
          type: OrganizationIdentityType.EMAIL,
          platform: this.platform,
          value: email,
          verified: false,
        })
      }
    }

    const keyMap: Record<string, string> = {
      name: 'name',
      description: 'headline',
      location: 'location',
      type: 'type',
      logo: 'logo',
      founded_year: 'founded',
      employee_count: 'employees',
      revenue: 'revenue',
    }

    const attributes: Record<string, unknown> = {}

    for (const [sourceKey, targetKey] of Object.entries(keyMap)) {
      const value = data[sourceKey as keyof typeof data]
      if (value !== undefined && value !== null) {
        attributes[targetKey] = value
      }
    }

    if (attributes.revenue) {
      const revenueRange = this.parseRevenue(attributes.revenue as string)
      if (revenueRange) {
        attributes.revenueRange = revenueRange
      }
    }

    if (data.parent_company?.name) {
      attributes.ultimateParent = data.parent_company.name
    }

    return {
      displayName: data.name,
      identities,
      attributes,
    }
  }

  private parseRevenue(revenue: string): { min: number; max: number } | null {
    try {
      const cleaned = revenue.replace(/\$/g, '').trim()

      if (cleaned.includes('-')) {
        const parts = cleaned.split('-').map((p) => p.trim())
        const min = this.convertRevenueToMillions(parts[0])
        const max = this.convertRevenueToMillions(parts[1])
        return { min, max }
      } else {
        const value = this.convertRevenueToMillions(cleaned)
        return {
          min: Math.round(value * 0.8),
          max: Math.round(value * 1.2),
        }
      }
    } catch {
      this.log.warn({ revenue }, 'Failed to parse revenue')
      return null
    }
  }

  private convertRevenueToMillions(value: string): number {
    const multiple = value.toUpperCase().endsWith('B') ? 1000 : 1
    const numValue = parseFloat(value.replace(/[^0-9.]/g, ''))
    return numValue * multiple
  }
}
