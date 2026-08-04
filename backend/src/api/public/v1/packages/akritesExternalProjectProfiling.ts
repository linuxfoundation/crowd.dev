import type {
  ReportingProtocolGuidelines,
  ReportingProtocolMethod,
  ReportingProtocolRow,
} from '@crowd/data-access-layer'

export interface AkritesExternalProjectProfiling {
  purl: string
  // Whether a reporting protocol was explicitly declared (vs. purely inferred from contacts).
  declared: boolean
  methods: ReportingProtocolMethod[]
  guidelines: ReportingProtocolGuidelines | null
  sources: Array<Record<string, unknown>>
  bugBountyUrl: string | null
  assembledAt: string | null
}

export interface ProjectProfilingBulkEntry {
  requestedPurl: string
  found: boolean
  profiling: AkritesExternalProjectProfiling | null
}

// Timestamptz columns arrive as the raw Postgres string (OID 1184 parser returns it
// verbatim), so normalize to canonical ISO 8601. Returns null for null/unparseable.
function toIsoOrNull(value: string | null): string | null {
  if (value == null) return null
  const ms = Date.parse(value)
  return Number.isNaN(ms) ? null : new Date(ms).toISOString()
}

export function toAkritesExternalProjectProfiling(
  row: ReportingProtocolRow,
): AkritesExternalProjectProfiling {
  return {
    purl: row.purl,
    declared: row.declared,
    methods: row.methods ?? [],
    guidelines: row.guidelines ?? null,
    sources: row.sources ?? [],
    bugBountyUrl: row.bugBountyUrl ?? null,
    assembledAt: toIsoOrNull(row.assembledAt),
  }
}
