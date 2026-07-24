import {
  type AkritesExternalContactDetailRow,
  type SecurityContactConfidence,
  securityContactConfidenceBand,
} from '@crowd/data-access-layer'

import { toNullableNumber } from './mappers'

// Both the per-contact band and the aggregate band mirror the internal
// SecurityContactConfidence enum verbatim (PRIMARY/SECONDARY/FALLBACK/NONE) —
// no external crosswalk (confirmed with product).
export type AkritesExternalContactConfidenceBand = SecurityContactConfidence
export type AkritesExternalOverallConfidenceBand = SecurityContactConfidence

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
  declaredRepositoryUrl: string | null
  resolvedRepositoryUrl: string | null
  repoMappingConfidence: number | null
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
  // packageConfidence), returned verbatim. NONE when there are no contacts.
  const overallConfidenceBand: AkritesExternalOverallConfidenceBand =
    contacts.length > 0
      ? securityContactConfidenceBand(Math.max(...contacts.map((c) => c.confidenceScore)))
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
    declaredRepositoryUrl: row.declaredRepositoryUrl ?? null,
    resolvedRepositoryUrl: row.resolvedRepositoryUrl ?? null,
    repoMappingConfidence: toNullableNumber(row.repoMappingConfidence),
    targetOrganizationName: null,
    bugBountyProgramFlag: null,
    reportingMethods: null,
    reportingGuidelines: null,
    integrationHints: null,
  }
}
