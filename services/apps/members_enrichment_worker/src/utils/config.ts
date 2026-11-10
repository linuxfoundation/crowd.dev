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

/**
 * Sources that look up their inputs (e.g. a LinkedIn handle) in other sources' cached data.
 * They run after the sources they read from, so a member's first enrichment can use them.
 */
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

/**
 * Share of each run reserved for members that were never enriched.
 * The rest goes to refreshing existing members, and either side can use the other's unused slots.
 */
export const NEW_MEMBERS_SHARE = 0.6
