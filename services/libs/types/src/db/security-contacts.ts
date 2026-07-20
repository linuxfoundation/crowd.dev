// security_contacts: per-repo security-contact rows (channel/value/role), scored and
// banded. See backend/src/osspckgs/migrations/V1782950400__security_contacts.sql and
// V1783036800__security_contacts_soft_delete.sql (adds deleted_at).
export type SecurityContactConfidence = 'PRIMARY' | 'SECONDARY' | 'FALLBACK' | 'NONE'

export interface SecurityContactDbRow {
  id: string
  repoId: string
  channel: string
  value: string
  role: string
  name: string | null
  // NUMERIC(4,3) — @crowd/database registers a global type-parser override for OID 1700
  // (parseFloat), so this comes back as a number, not the raw-string pg-promise default
  // (same convention as PackageDbRow.impact — a NUMERIC column typed as number | null).
  score: number
  confidence: SecurityContactConfidence
  provenance: unknown[]
  lastRefreshed: string
  createdAt: string
  updatedAt: string
  deletedAt: string | null
}
