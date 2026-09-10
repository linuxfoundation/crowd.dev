import { MemberAttributeName } from '@crowd/types'

export const ALSO_USE_EMAIL_IDENTITIES_FOR_ENRICHMENT = false
export const ENRICH_EMAIL_IDENTITIES = false

/**
 * Attributes safe to write from a single enrichment source.
 */
export const SINGLE_SOURCE_ENRICHMENT_ATTRIBUTES: MemberAttributeName[] = [
  MemberAttributeName.LOCATION,
  MemberAttributeName.COUNTRY,
]
