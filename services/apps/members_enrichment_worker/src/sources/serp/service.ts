import axios from 'axios'

import { Logger, LoggerBase } from '@crowd/logging'
import { MemberEnrichmentSource, MemberIdentityType, PlatformType } from '@crowd/types'

import {
  IEnrichmentService,
  IEnrichmentSourceInput,
  IMemberEnrichmentDataNormalized,
} from '../../types'

import {
  IMemberEnrichmentDataSerp,
  IMemberEnrichmentSerpApiResponse,
  ISerpApiAccountUsageData,
} from './types'

export default class EnrichmentServiceSerpApi extends LoggerBase implements IEnrichmentService {
  public source: MemberEnrichmentSource = MemberEnrichmentSource.SERP
  public platform = `enrichment-${this.source}`
  public enrichMembersWithActivityMoreThan = 500

  public enrichableBySql = `
  ("membersGlobalActivityCount".total_count > ${this.enrichMembersWithActivityMoreThan}) AND
  (members."displayName" like '% %') AND 
  (members.attributes->'location'->>'default' is not null and members.attributes->'location'->>'default' <> '') AND
  ((members.attributes->'websiteUrl'->>'default' is not null and members.attributes->'websiteUrl'->>'default' <> '') OR 
   (mi.verified AND mi.type = 'username' and mi.platform = 'github') OR 
   (mi.verified AND mi.type = 'email')
  )`

  // bust cache after 120 days
  public cacheObsoleteAfterSeconds = 60 * 60 * 24 * 120

  public maxConcurrentRequests = 10

  constructor(public readonly log: Logger) {
    super(log)
  }

  async isEnrichableBySource(input: IEnrichmentSourceInput): Promise<boolean> {
    const displayNameSplit = input.displayName?.split(' ')
    return (
      displayNameSplit?.length > 1 &&
      input.activityCount &&
      input.activityCount > this.enrichMembersWithActivityMoreThan &&
      !!input.location &&
      ((!!input.email && input.email.verified) ||
        (!!input.github && input.github.verified) ||
        !!input.website)
    )
  }

  async hasRemainingCredits(): Promise<boolean> {
    try {
      const config = {
        method: 'get',
        url: `https://serpapi.com/account`,
        params: {
          api_key: process.env['CROWD_ENRICHMENT_SERP_API_KEY'],
        },
      }

      const response: ISerpApiAccountUsageData = (await axios(config)).data

      return response.total_searches_left > 0
    } catch (error) {
      this.log.error('Error while checking serpapi account usage', error)
      return false
    }
  }

  async getData(input: IEnrichmentSourceInput): Promise<IMemberEnrichmentDataSerp | null> {
    let enriched: IMemberEnrichmentDataSerp = null

    if (input.displayName && input.location && input.website) {
      enriched = await this.querySerpApi(input.displayName, input.location, input.website)
    }

    if (!enriched && input.displayName && input.location && input.github && input.github.value) {
      enriched = await this.querySerpApi(input.displayName, input.location, input.github.value)
    }

    if (!enriched && input.displayName && input.location && input.email && input.email.value) {
      enriched = await this.querySerpApi(input.displayName, input.location, input.email.value)
    }
    return enriched
  }

  private async querySerpApi(
    displayName: string,
    location: string,
    identifier: string,
  ): Promise<IMemberEnrichmentDataSerp> {
    const config = {
      method: 'get',
      url: process.env['CROWD_ENRICHMENT_SERP_API_URL'],
      params: {
        api_key: process.env['CROWD_ENRICHMENT_SERP_API_KEY'],
        q: `"${displayName}" ${location} "${identifier}" site:linkedin.com/in`,
        num: 3,
        engine: 'google',
      },
      validateStatus: function (status) {
        return (status >= 200 && status < 300) || status === 404 || status === 422
      },
    }

    const response = await axios<IMemberEnrichmentSerpApiResponse>(config)

    if (response.status === 404 || response.status === 422) {
      this.log.debug(
        { source: this.source, displayName, location, identifier },
        'No data found for search!',
      )
      return null
    }

    if (!response.data?.organic_results) {
      return null
    }

    if (response.data.search_information.total_results > 0) {
      if (
        response.data.organic_results.length > 0 &&
        response.data.organic_results[0].link &&
        !response.data.search_information.spelling_fix &&
        !response.data.search_information.spelling_fix_type
      ) {
        return {
          linkedinUrl: response.data.organic_results[0].link,
        }
      }
    }

    return null
  }

  normalize(data: IMemberEnrichmentDataSerp): IMemberEnrichmentDataNormalized {
    const normalized: IMemberEnrichmentDataNormalized = {
      identities: [
        {
          platform: PlatformType.LINKEDIN,
          type: MemberIdentityType.USERNAME,
          verified: false,
          value: this.getLinkedInProfileHandle(this.normalizeLinkedUrl(data.linkedinUrl)),
          source: 'enrichment',
        },
      ],
    }
    return normalized
  }

  private getLinkedInProfileHandle(url: string): string | null {
    const regex = /in\/([^/]+)/
    const match = url.match(regex)
    return match ? match[1] : null
  }

  private normalizeLinkedUrl(url: string): string {
    try {
      const parsedUrl = new URL(url)

      if (parsedUrl.hostname.endsWith('linkedin.com')) {
        parsedUrl.hostname = 'linkedin.com'
        parsedUrl.search = ''

        let path = parsedUrl.pathname
        if (path.endsWith('/')) {
          path = path.slice(0, -1)
        }

        return parsedUrl.origin + path
      }

      return url
    } catch (error) {
      this.log.error(`Error while normalizing linkedin url: ${url}`, error)
      throw error
    }
  }
}
