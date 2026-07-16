import {
  type AkritesExternalContactDetailRow,
  type SecurityContactConfidence,
  securityContactConfidenceBand,
} from '@crowd/data-access-layer'

// Per-contact band mirrors the internal SecurityContactConfidence enum verbatim.
export type AkritesExternalContactConfidenceBand = SecurityContactConfidence
// Aggregate band is a DIFFERENT scale in the contract (HIGH/MEDIUM/LOW/NONE).
export type AkritesExternalOverallConfidenceBand = 'HIGH' | 'MEDIUM' | 'LOW' | 'NONE'

export interface AkritesExternalSecurityContact {
  channel: string
  value: string
  role: string
  confidenceBand: AkritesExternalContactConfidenceBand
  confidenceScore: number
}

export interface AkritesExternalContactDetail {
  purl: string
  name: string
  ecosystem: string
  overallConfidenceBand: AkritesExternalOverallConfidenceBand
  contacts: AkritesExternalSecurityContact[]
  securityPolicyUrl: string | null
  vulnerabilityReportingUrl: string | null
  bugBountyUrl: string | null
  pvrEnabled: boolean | null
  // Reserved by the contract but not stored today — always null (see the akrites-external
  // OpenAPI notes). Typed as null so a future non-null shape is an explicit, reviewed change.
  targetOrganizationName: null
  bugBountyProgramFlag: null
  reportingMethods: null
  reportingGuidelines: null
  integrationHints: null
}

export interface ContactDetailBulkEntry {
  requestedPurl: string
  found: boolean
  contact: AkritesExternalContactDetail | null
}

// UNCONFIRMED CROSSWALK — the internal aggregate band is PRIMARY/SECONDARY/FALLBACK/NONE
// (securityContactConfidenceBand), but the Akrites contract's overallConfidenceBand uses
// HIGH/MEDIUM/LOW/NONE. This is a clean 4→4 tier rename, but the contract flags it as an
// open question (there is no existing function producing the HIGH/… scale). Confirm with
// Akrites/product; if they'd rather reuse PRIMARY/SECONDARY/FALLBACK/NONE, drop this table.
const OVERALL_CONFIDENCE_CROSSWALK: Record<
  SecurityContactConfidence,
  AkritesExternalOverallConfidenceBand
> = {
  PRIMARY: 'HIGH',
  SECONDARY: 'MEDIUM',
  FALLBACK: 'LOW',
  NONE: 'NONE',
}

export function toAkritesExternalContactDetail(
  row: AkritesExternalContactDetailRow,
): AkritesExternalContactDetail {
  const contacts: AkritesExternalSecurityContact[] = (row.securityContacts ?? []).map((c) => ({
    channel: c.channel,
    value: c.value,
    role: c.role,
    confidenceBand: c.confidence,
    confidenceScore: Number(c.score),
  }))

  // Aggregate band derives from the highest-scoring contact (same rule as the internal
  // packageConfidence), then crosswalked to the contract's scale. NONE when there are none.
  const overallConfidenceBand =
    contacts.length > 0
      ? OVERALL_CONFIDENCE_CROSSWALK[
          securityContactConfidenceBand(Math.max(...contacts.map((c) => c.confidenceScore)))
        ]
      : 'NONE'

  return {
    purl: row.purl,
    name: row.name,
    ecosystem: row.ecosystem,
    overallConfidenceBand,
    contacts,
    securityPolicyUrl: row.securityPolicyUrl ?? null,
    vulnerabilityReportingUrl: row.vulnerabilityReportingUrl ?? null,
    bugBountyUrl: row.bugBountyUrl ?? null,
    pvrEnabled: row.pvrEnabled ?? null,
    targetOrganizationName: null,
    bugBountyProgramFlag: null,
    reportingMethods: null,
    reportingGuidelines: null,
    integrationHints: null,
  }
}
