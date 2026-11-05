import { canonicalCdpOrganizationIdByMemberName } from './akritesMembers'
import type { AffiliationConfidence } from './identityAffiliations'
import type {
  AffiliatedMemberIdentityRow,
  MemberContributorsPackageRow,
  MemberIdentitySource,
} from './memberContributors'

function toNullableNumber(value: number | string | null): number | null {
  return value != null ? Number(value) : null
}

const CANONICAL_ORGANIZATION_IDS = canonicalCdpOrganizationIdByMemberName()

export interface AkritesExternalMemberOrganization {
  id: string
  name: string
  confidence: AffiliationConfidence
  confidenceScore: number
}

export interface AkritesExternalMemberEntry {
  githubLogin: string | null
  displayName: string | null
  emails: string[]
  registryUsernames: string[]
  sources: MemberIdentitySource[]
  organization: AkritesExternalMemberOrganization
}

export interface AkritesExternalMemberRole {
  role: string
  roleKind: string
  current: boolean
}

export interface AkritesExternalMaintainer extends AkritesExternalMemberEntry {
  roles: AkritesExternalMemberRole[]
}

export interface MemberContributorsSections {
  contributors: AkritesExternalMemberEntry[]
  registry: AkritesExternalMaintainer[]
  governanceFiles: AkritesExternalMaintainer[]
}

export type LinkedMemberContributorsPackageRow = Omit<MemberContributorsPackageRow, 'repoUrl'> & {
  repoUrl: string
}

export interface AkritesExternalMemberContributors {
  purl: string
  name: string
  ecosystem: string
  repoUrl: string
  repoMappingConfidence: number | null
  status: 'collected' | 'not_collected'
  contributors: AkritesExternalMemberEntry[]
  maintainers: {
    registry: AkritesExternalMaintainer[]
    governanceFiles: AkritesExternalMaintainer[]
  }
}

type AffiliatedRow = AffiliatedMemberIdentityRow & {
  akritesMember: string
  cdpOrganizationId: string
  confidence: AffiliationConfidence
  confidenceScore: string | number
}

const GITHUB_LOGIN_SHAPE = /^[a-z0-9](?:[a-z0-9]|-(?=[a-z0-9])){0,38}$/

const CONFIDENCE_METHOD_RANK: Record<AffiliationConfidence, number> = {
  cdp: 0,
  email_domain: 1,
  github_company: 2,
}

const CONTRIBUTOR_SOURCES: ReadonlySet<MemberIdentitySource> = new Set(['git_activity', 'github'])

function compareStrings(a: string, b: string): number {
  if (a < b) return -1
  if (a > b) return 1
  return 0
}

function compareNullableStrings(a: string | null, b: string | null): number {
  if (a === b) return 0
  if (a === null) return 1
  if (b === null) return -1
  return compareStrings(a, b)
}

function compareRecentFirst(a: string | null, b: string | null): number {
  if (a === null && b === null) return 0
  if (a === null) return 1
  if (b === null) return -1
  return Date.parse(b) - Date.parse(a)
}

function distinctSorted<T extends string>(values: (T | null)[]): T[] {
  const present = values.filter((value): value is T => value !== null)
  return [...new Set(present)].sort(compareStrings)
}

function isAffiliated(row: AffiliatedMemberIdentityRow): row is AffiliatedRow {
  return (
    row.akritesMember !== null &&
    row.cdpOrganizationId !== null &&
    row.confidence !== null &&
    row.confidenceScore !== null
  )
}

function groupByPerson(
  rows: AffiliatedMemberIdentityRow[],
): Map<string, AffiliatedMemberIdentityRow[]> {
  const groups = new Map<string, AffiliatedMemberIdentityRow[]>()
  for (const row of rows) {
    const group = groups.get(row.person)
    if (group) group.push(row)
    else groups.set(row.person, [row])
  }
  return groups
}

function compareLoginRows(a: AffiliatedMemberIdentityRow, b: AffiliatedMemberIdentityRow): number {
  const shapeOrder =
    Number(GITHUB_LOGIN_SHAPE.test(b.identityValue)) -
    Number(GITHUB_LOGIN_SHAPE.test(a.identityValue))
  return (
    shapeOrder ||
    compareRecentFirst(a.lastSeenAt, b.lastSeenAt) ||
    compareStrings(a.identityValue, b.identityValue)
  )
}

function pickGithubLogin(rows: AffiliatedMemberIdentityRow[]): string | null {
  const [best] = rows.filter((row) => row.identityType === 'github-login').sort(compareLoginRows)
  return best?.identityValue ?? null
}

function pickDisplayName(rows: AffiliatedMemberIdentityRow[]): string | null {
  const [latestAuthorName] = rows
    .filter((row) => row.identityType === 'git-author-name')
    .sort((a, b) => compareRecentFirst(a.lastSeenAt, b.lastSeenAt))
  if (latestAuthorName) return latestAuthorName.identityValue
  return rows.find((row) => row.registryDisplayName !== null)?.registryDisplayName ?? null
}

function compareAffiliations(a: AffiliatedRow, b: AffiliatedRow): number {
  return (
    Number(b.confidenceScore) - Number(a.confidenceScore) ||
    compareRecentFirst(a.lastSeenAt, b.lastSeenAt) ||
    CONFIDENCE_METHOD_RANK[a.confidence] - CONFIDENCE_METHOD_RANK[b.confidence]
  )
}

function pickOrganization(
  rows: AffiliatedMemberIdentityRow[],
  canonicalIds: Map<string, string>,
): AkritesExternalMemberOrganization | null {
  const [best] = rows.filter(isAffiliated).sort(compareAffiliations)
  if (!best) return null
  return {
    id: canonicalIds.get(best.akritesMember) ?? best.cdpOrganizationId,
    name: best.akritesMember,
    confidence: best.confidence,
    confidenceScore: Number(best.confidenceScore),
  }
}

function toMemberEntry(
  rows: AffiliatedMemberIdentityRow[],
  canonicalIds: Map<string, string>,
): AkritesExternalMemberEntry | null {
  const organization = pickOrganization(rows, canonicalIds)
  if (!organization) return null
  return {
    githubLogin: pickGithubLogin(rows),
    displayName: pickDisplayName(rows),
    emails: distinctSorted(
      rows.map((row) => (row.identityType === 'email' ? row.identityValue : null)),
    ),
    registryUsernames: distinctSorted(rows.map((row) => row.registryUsername)),
    sources: distinctSorted(rows.map((row) => row.source)),
    organization,
  }
}

function registryRoles(rows: AffiliatedMemberIdentityRow[]): AkritesExternalMemberRole[] {
  return distinctSorted(rows.filter((row) => row.source === 'registry').map((row) => row.role)).map(
    (role) => ({ role, roleKind: 'maintainer', current: true }),
  )
}

function isGovernanceRoleRow(
  row: AffiliatedMemberIdentityRow,
): row is AffiliatedMemberIdentityRow & { role: string } {
  return row.source === 'governance_file' && row.role !== null
}

function governanceRoles(rows: AffiliatedMemberIdentityRow[]): AkritesExternalMemberRole[] {
  const byRole = new Map<string, AkritesExternalMemberRole>()
  for (const row of rows.filter(isGovernanceRoleRow)) {
    const key = `${row.role}\u0000${row.roleKind}`
    const current = row.endedAt === null && row.roleKind !== 'emeritus'
    const existing = byRole.get(key)
    if (existing) {
      existing.current = existing.current || current
    } else {
      byRole.set(key, { role: row.role, roleKind: row.roleKind, current })
    }
  }
  return [...byRole.values()].sort((a, b) => compareStrings(a.role, b.role))
}

function compareEntries(a: AkritesExternalMemberEntry, b: AkritesExternalMemberEntry): number {
  return (
    compareStrings(a.organization.name, b.organization.name) ||
    compareNullableStrings(a.githubLogin, b.githubLogin) ||
    compareNullableStrings(a.emails[0] ?? null, b.emails[0] ?? null)
  )
}

function hasSource(
  rows: AffiliatedMemberIdentityRow[],
  matches: (source: MemberIdentitySource) => boolean,
): boolean {
  return rows.some((row) => matches(row.source))
}

export function groupAffiliatedMemberIdentities(
  rows: AffiliatedMemberIdentityRow[],
  canonicalIds: Map<string, string> = CANONICAL_ORGANIZATION_IDS,
): MemberContributorsSections {
  const sections: MemberContributorsSections = {
    contributors: [],
    registry: [],
    governanceFiles: [],
  }
  for (const personRows of groupByPerson(rows).values()) {
    const entry = toMemberEntry(personRows, canonicalIds)
    if (!entry) continue
    if (hasSource(personRows, (source) => CONTRIBUTOR_SOURCES.has(source))) {
      sections.contributors.push(entry)
    }
    if (hasSource(personRows, (source) => source === 'registry')) {
      sections.registry.push({ ...entry, roles: registryRoles(personRows) })
    }
    if (hasSource(personRows, (source) => source === 'governance_file')) {
      sections.governanceFiles.push({ ...entry, roles: governanceRoles(personRows) })
    }
  }
  return {
    contributors: sections.contributors.sort(compareEntries),
    registry: sections.registry.sort(compareEntries),
    governanceFiles: sections.governanceFiles.sort(compareEntries),
  }
}

export function toAkritesExternalMemberContributors(
  pkg: LinkedMemberContributorsPackageRow,
  sections: MemberContributorsSections,
): AkritesExternalMemberContributors {
  return {
    purl: pkg.purl,
    name: pkg.name,
    ecosystem: pkg.ecosystem,
    repoUrl: pkg.repoUrl,
    repoMappingConfidence: toNullableNumber(pkg.repoMappingConfidence),
    status: pkg.hasContributorRows ? 'collected' : 'not_collected',
    contributors: pkg.hasContributorRows ? sections.contributors : [],
    maintainers: {
      registry: sections.registry,
      governanceFiles: pkg.hasContributorRows ? sections.governanceFiles : [],
    },
  }
}

export interface InferredRepoMemberContributorsBody extends Omit<
  AkritesExternalMemberContributors,
  'status' | 'repoMappingConfidence'
> {
  repoMappingConfidence: null
  repoUrlSource: 'llm_inferred'
}

export interface InferredRepoPackage {
  purl: string
  name: string
  ecosystem: string
  repoUrl: string
}

export function toInferredRepoMemberContributorsBody(
  pkg: InferredRepoPackage,
  sections: MemberContributorsSections,
): InferredRepoMemberContributorsBody {
  return {
    purl: pkg.purl,
    name: pkg.name,
    ecosystem: pkg.ecosystem,
    repoUrl: pkg.repoUrl,
    repoUrlSource: 'llm_inferred',
    repoMappingConfidence: null,
    contributors: sections.contributors,
    maintainers: { registry: [], governanceFiles: [] },
  }
}
