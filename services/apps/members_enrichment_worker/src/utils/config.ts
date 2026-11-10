import { MemberAttributeName, MemberEnrichmentSource } from '@crowd/types'

export const ALSO_USE_EMAIL_IDENTITIES_FOR_ENRICHMENT = false
export const ENRICH_EMAIL_IDENTITIES = false

/**
 * Attributes safe to write from a single enrichment source.
 */
export const SINGLE_SOURCE_ENRICHMENT_ATTRIBUTES: MemberAttributeName[] = [
  MemberAttributeName.LOCATION,
  MemberAttributeName.COUNTRY,
]

export const SOURCES_READING_OTHER_CACHES: Partial<
  Record<MemberEnrichmentSource, MemberEnrichmentSource[]>
> = {
  [MemberEnrichmentSource.PROGAI_LINKEDIN_SCRAPER]: [
    MemberEnrichmentSource.PROGAI,
    MemberEnrichmentSource.CLEARBIT,
    MemberEnrichmentSource.SERP,
  ],
  [MemberEnrichmentSource.CRUSTDATA]: [
    MemberEnrichmentSource.PROGAI,
    MemberEnrichmentSource.CLEARBIT,
    MemberEnrichmentSource.SERP,
  ],
}

export const NEW_MEMBERS_SHARE = 0.6
