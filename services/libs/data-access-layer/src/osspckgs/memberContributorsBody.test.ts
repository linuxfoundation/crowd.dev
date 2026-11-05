import { describe, expect, it } from 'vitest'

import type {
  AffiliatedMemberIdentityRow,
  MemberContributorsPackageRow,
  MemberIdentitySource,
} from './memberContributors'
import {
  groupAffiliatedMemberIdentities,
  toAkritesExternalMemberContributors,
  toInferredRepoMemberContributorsBody,
} from './memberContributorsBody'

const GOOGLE = {
  akritesMember: 'Google',
  cdpOrganizationId: 'org-google',
  confidence: 'cdp' as const,
  confidenceScore: '1.0',
}
const IBM = {
  akritesMember: 'IBM',
  cdpOrganizationId: 'org-ibm',
  confidence: 'email_domain' as const,
  confidenceScore: '0.7',
}
const NO_CANONICAL_IDS = new Map<string, string>()
const UNAFFILIATED = {
  akritesMember: null,
  cdpOrganizationId: null,
  confidence: null,
  confidenceScore: null,
}

function row(overrides: Partial<AffiliatedMemberIdentityRow> = {}): AffiliatedMemberIdentityRow {
  return {
    person: 'member-1',
    source: 'git_activity',
    role: null,
    roleKind: 'contributor',
    identityType: 'github-login',
    identityValue: 'alice',
    endedAt: null,
    lastSeenAt: '2026-01-01T00:00:00Z',
    registryUsername: null,
    registryDisplayName: null,
    ...GOOGLE,
    ...overrides,
  }
}

function pkgRow(
  overrides: Partial<MemberContributorsPackageRow> = {},
): MemberContributorsPackageRow {
  return {
    packageId: 'pkg-1',
    purl: 'pkg:npm/express',
    name: 'express',
    ecosystem: 'npm',
    repoId: 'repo-1',
    repoUrl: 'https://github.com/expressjs/express',
    repoMappingConfidence: '0.9',
    hasContributorRows: true,
    ...overrides,
  }
}

describe('groupAffiliatedMemberIdentities', () => {
  it('returns empty sections for no rows', () => {
    expect(groupAffiliatedMemberIdentities([])).toEqual({
      contributors: [],
      registry: [],
      governanceFiles: [],
    })
  })

  it('places a person in every section it has rows for with merged identities', () => {
    const rows = [
      row({ source: 'git_activity', identityType: 'github-login', identityValue: 'alice' }),
      row({
        source: 'git_activity',
        identityType: 'email',
        identityValue: 'alice@google.com',
        ...UNAFFILIATED,
      }),
      row({
        source: 'github',
        identityType: 'email',
        identityValue: 'alice@example.org',
        ...UNAFFILIATED,
      }),
      row({
        source: 'registry',
        role: 'maintainer',
        roleKind: 'registry',
        identityType: 'registry-username',
        identityValue: 'alice-npm',
        lastSeenAt: null,
        registryUsername: 'alice-npm',
        registryDisplayName: 'Alice A.',
        ...UNAFFILIATED,
      }),
    ]

    const { contributors, registry, governanceFiles } = groupAffiliatedMemberIdentities(
      rows,
      NO_CANONICAL_IDS,
    )

    const base = {
      githubLogin: 'alice',
      displayName: 'Alice A.',
      emails: ['alice@example.org', 'alice@google.com'],
      registryUsernames: ['alice-npm'],
      sources: ['git_activity', 'github', 'registry'],
      organization: { id: 'org-google', name: 'Google', confidence: 'cdp', confidenceScore: 1 },
    }
    expect(contributors).toEqual([base])
    expect(registry).toEqual([
      { ...base, roles: [{ role: 'maintainer', roleKind: 'maintainer', current: true }] },
    ])
    expect(governanceFiles).toEqual([])
  })

  it('skips persons without any affiliated row', () => {
    const rows = [row({ person: 'ghost', ...UNAFFILIATED })]
    expect(groupAffiliatedMemberIdentities(rows).contributors).toEqual([])
  })

  describe('organization choice', () => {
    it('keeps the higher score across sources (cdp Google over email_domain IBM)', () => {
      const rows = [
        row({ source: 'git_activity', identityValue: 'alice', lastSeenAt: null, ...GOOGLE }),
        row({
          source: 'registry',
          roleKind: 'identity',
          identityType: 'email',
          identityValue: 'alice@ibm.com',
          lastSeenAt: '2026-06-01T00:00:00Z',
          ...IBM,
        }),
      ]
      const [entry] = groupAffiliatedMemberIdentities(rows, NO_CANONICAL_IDS).contributors
      expect(entry.organization).toEqual({
        id: 'org-google',
        name: 'Google',
        confidence: 'cdp',
        confidenceScore: 1,
      })
    })

    it('returns the member canonical organization id instead of the matched one', () => {
      const canonicalIds = new Map([['Google', 'org-google-canonical']])
      const [entry] = groupAffiliatedMemberIdentities(
        [row({ ...GOOGLE })],
        canonicalIds,
      ).contributors
      expect(entry.organization.id).toBe('org-google-canonical')
    })

    it('keeps the matched organization id when the member is not in the registry', () => {
      const canonicalIds = new Map([['Google', 'org-google-canonical']])
      const [entry] = groupAffiliatedMemberIdentities([row({ ...IBM })], canonicalIds).contributors
      expect(entry.organization.id).toBe('org-ibm')
    })

    it('emits the score as a number when pg returns a string', () => {
      const [entry] = groupAffiliatedMemberIdentities([row({ ...IBM })]).contributors
      expect(entry.organization.confidenceScore).toBe(0.7)
      expect(typeof entry.organization.confidenceScore).toBe('number')
    })

    it('breaks score ties by most recent lastSeenAt, nulls last', () => {
      const rows = [
        row({ identityType: 'email', identityValue: 'a@ibm.com', lastSeenAt: null, ...IBM }),
        row({
          identityType: 'email',
          identityValue: 'a@redhat.com',
          lastSeenAt: '2025-01-01T00:00:00Z',
          ...IBM,
          akritesMember: 'Red Hat',
          cdpOrganizationId: 'org-redhat',
        }),
        row({
          identityType: 'email',
          identityValue: 'a@intel.com',
          lastSeenAt: '2024-01-01T00:00:00Z',
          ...IBM,
          akritesMember: 'Intel',
          cdpOrganizationId: 'org-intel',
        }),
      ]
      const [entry] = groupAffiliatedMemberIdentities(rows).contributors
      expect(entry.organization.name).toBe('Red Hat')
    })

    it('breaks score and recency ties by method cdp, email_domain, github_company', () => {
      const seen = '2026-01-01T00:00:00Z'
      const rows = [
        row({
          identityValue: 'alice',
          lastSeenAt: seen,
          akritesMember: 'Intel',
          cdpOrganizationId: 'org-intel',
          confidence: 'github_company',
          confidenceScore: '0.7',
        }),
        row({
          identityType: 'email',
          identityValue: 'a@ibm.com',
          lastSeenAt: seen,
          ...IBM,
        }),
      ]
      const [entry] = groupAffiliatedMemberIdentities(rows).contributors
      expect(entry.organization.name).toBe('IBM')
    })
  })

  describe('githubLogin', () => {
    it('prefers a GitHub-shaped login over a non-shaped one', () => {
      const rows = [
        row({ identityValue: 'paul serby', lastSeenAt: '2026-06-01T00:00:00Z' }),
        row({ identityValue: 'serby', lastSeenAt: '2020-01-01T00:00:00Z' }),
      ]
      const [entry] = groupAffiliatedMemberIdentities(rows).contributors
      expect(entry.githubLogin).toBe('serby')
    })

    it('prefers the most recently seen shaped login, then alphabetical', () => {
      const recent = groupAffiliatedMemberIdentities([
        row({ identityValue: 'aaa', lastSeenAt: null }),
        row({ identityValue: 'zzz', lastSeenAt: '2026-06-01T00:00:00Z' }),
        row({ identityValue: 'mmm', lastSeenAt: '2025-06-01T00:00:00Z' }),
      ]).contributors[0]
      expect(recent.githubLogin).toBe('zzz')

      const alphabetical = groupAffiliatedMemberIdentities([
        row({ identityValue: 'zzz', lastSeenAt: null }),
        row({ identityValue: 'aaa', lastSeenAt: null }),
      ]).contributors[0]
      expect(alphabetical.githubLogin).toBe('aaa')
    })

    it('is null when the person has no github-login identity', () => {
      const [entry] = groupAffiliatedMemberIdentities([
        row({ identityType: 'email', identityValue: 'a@google.com' }),
      ]).contributors
      expect(entry.githubLogin).toBeNull()
    })
  })

  describe('displayName', () => {
    it('uses the latest git-author-name first', () => {
      const rows = [
        row(),
        row({
          identityType: 'git-author-name',
          identityValue: 'Old Name',
          lastSeenAt: '2020-01-01T00:00:00Z',
          ...UNAFFILIATED,
        }),
        row({
          identityType: 'git-author-name',
          identityValue: 'New Name',
          lastSeenAt: '2026-01-01T00:00:00Z',
          ...UNAFFILIATED,
        }),
        row({
          source: 'registry',
          roleKind: 'registry',
          identityType: 'registry-username',
          identityValue: 'alice-npm',
          registryUsername: 'alice-npm',
          registryDisplayName: 'Registry Name',
          ...UNAFFILIATED,
        }),
      ]
      expect(groupAffiliatedMemberIdentities(rows).contributors[0].displayName).toBe('New Name')
    })

    it('falls back to the registry display name', () => {
      const rows = [
        row(),
        row({
          source: 'registry',
          roleKind: 'registry',
          identityType: 'registry-username',
          identityValue: 'alice-npm',
          registryUsername: 'alice-npm',
          registryDisplayName: 'Registry Name',
          ...UNAFFILIATED,
        }),
      ]
      expect(groupAffiliatedMemberIdentities(rows).contributors[0].displayName).toBe(
        'Registry Name',
      )
    })

    it('is null when neither exists', () => {
      expect(groupAffiliatedMemberIdentities([row()]).contributors[0].displayName).toBeNull()
    })
  })

  it('maps governance roles with the current flag sorted by role', () => {
    const rows = [
      row({ source: 'governance_file', role: 'tc-member', roleKind: 'maintainer', endedAt: null }),
      row({ source: 'governance_file', role: 'emeritus', roleKind: 'emeritus', endedAt: null }),
      row({
        source: 'governance_file',
        role: 'lead',
        roleKind: 'maintainer',
        endedAt: '2025-01-01T00:00:00Z',
      }),
    ]
    const { governanceFiles, contributors } = groupAffiliatedMemberIdentities(rows)
    expect(contributors).toEqual([])
    expect(governanceFiles[0].roles).toEqual([
      { role: 'emeritus', roleKind: 'emeritus', current: false },
      { role: 'lead', roleKind: 'maintainer', current: false },
      { role: 'tc-member', roleKind: 'maintainer', current: true },
    ])
    expect(governanceFiles[0].sources).toEqual(['governance_file'])
  })

  it('merges the same governance role held through two identities into one current role', () => {
    const rows = [
      row({
        source: 'governance_file',
        role: 'maintainer',
        roleKind: 'maintainer',
        identityType: 'github-login',
        identityValue: 'alice',
        endedAt: '2025-01-01T00:00:00Z',
      }),
      row({
        source: 'governance_file',
        role: 'maintainer',
        roleKind: 'maintainer',
        identityType: 'email',
        identityValue: 'alice@google.com',
        endedAt: null,
      }),
    ]
    const { governanceFiles } = groupAffiliatedMemberIdentities(rows)
    expect(governanceFiles).toHaveLength(1)
    expect(governanceFiles[0].roles).toEqual([
      { role: 'maintainer', roleKind: 'maintainer', current: true },
    ])
  })

  it('maps registry roles as distinct current maintainer roles sorted by role', () => {
    const registryRow = {
      source: 'registry' as const,
      roleKind: 'registry',
      identityType: 'registry-username' as const,
      ...UNAFFILIATED,
    }
    const rows = [
      row({ source: 'registry', roleKind: 'identity', role: null }),
      row({ ...registryRow, role: 'owner', identityValue: 'a', registryUsername: 'a' }),
      row({ ...registryRow, role: 'maintainer', identityValue: 'b', registryUsername: 'b' }),
      row({ ...registryRow, role: 'owner', identityValue: 'c', registryUsername: 'c' }),
    ]
    const [entry] = groupAffiliatedMemberIdentities(rows).registry
    expect(entry.roles).toEqual([
      { role: 'maintainer', roleKind: 'maintainer', current: true },
      { role: 'owner', roleKind: 'maintainer', current: true },
    ])
    expect(entry.registryUsernames).toEqual(['a', 'b', 'c'])
  })

  it('sorts each list by organization name, then githubLogin nulls last, then first email', () => {
    const rows = [
      row({ person: 'p1', identityValue: 'zed', ...IBM }),
      row({ person: 'p2', identityValue: 'bob' }),
      row({ person: 'p3', identityType: 'email', identityValue: 'b@google.com' }),
      row({ person: 'p4', identityType: 'email', identityValue: 'a@google.com' }),
      row({ person: 'p5', identityValue: 'amy' }),
    ]
    const order = groupAffiliatedMemberIdentities(rows).contributors.map(
      (e) => e.githubLogin ?? e.emails[0],
    )
    expect(order).toEqual(['amy', 'bob', 'a@google.com', 'b@google.com', 'zed'])
  })
})

describe('toAkritesExternalMemberContributors', () => {
  it('maps a collected package with numeric repo mapping confidence', () => {
    const sections = groupAffiliatedMemberIdentities([row()])
    const result = toAkritesExternalMemberContributors(pkgRow(), sections)
    expect(result).toEqual({
      purl: 'pkg:npm/express',
      name: 'express',
      ecosystem: 'npm',
      repoUrl: 'https://github.com/expressjs/express',
      repoMappingConfidence: 0.9,
      status: 'collected',
      contributors: sections.contributors,
      maintainers: { registry: [], governanceFiles: [] },
    })
  })

  it('maps hasContributorRows false to not_collected', () => {
    const result = toAkritesExternalMemberContributors(
      pkgRow({ hasContributorRows: false, repoMappingConfidence: null }),
      { contributors: [], registry: [], governanceFiles: [] },
    )
    expect(result.status).toBe('not_collected')
    expect(result.repoMappingConfidence).toBeNull()
  })

  it('drops contributor and governance sections but keeps registry when not collected', () => {
    const sections = groupAffiliatedMemberIdentities([
      row({ source: 'git_activity', identityValue: 'alice' }),
      row({ person: 'p2', source: 'governance_file', role: 'lead', roleKind: 'maintainer' }),
      row({
        person: 'p3',
        source: 'registry',
        role: 'maintainer',
        roleKind: 'registry',
        identityType: 'registry-username',
        identityValue: 'carol',
        registryUsername: 'carol',
      }),
    ])
    const result = toAkritesExternalMemberContributors(
      pkgRow({ hasContributorRows: false }),
      sections,
    )
    expect(result.contributors).toEqual([])
    expect(result.maintainers.governanceFiles).toEqual([])
    expect(result.maintainers.registry).toHaveLength(1)
  })
})

describe('toInferredRepoMemberContributorsBody', () => {
  it('builds the sync body with llm provenance, null mapping confidence and no maintainers', () => {
    const entry = {
      githubLogin: 'octo',
      displayName: null,
      emails: [],
      registryUsernames: [],
      sources: ['github'] as MemberIdentitySource[],
      organization: {
        id: 'org-google',
        name: 'Google',
        confidence: 'cdp' as const,
        confidenceScore: 1,
      },
    }
    const body = toInferredRepoMemberContributorsBody(
      {
        purl: 'pkg:hackage/aeson',
        name: 'aeson',
        ecosystem: 'hackage',
        repoUrl: 'https://github.com/haskell/aeson',
      },
      {
        contributors: [entry],
        registry: [
          { ...entry, roles: [{ role: 'maintainer', roleKind: 'maintainer', current: true }] },
        ],
        governanceFiles: [],
      },
    )
    expect(body).toEqual({
      purl: 'pkg:hackage/aeson',
      name: 'aeson',
      ecosystem: 'hackage',
      repoUrl: 'https://github.com/haskell/aeson',
      repoUrlSource: 'llm_inferred',
      repoMappingConfidence: null,
      contributors: [entry],
      maintainers: { registry: [], governanceFiles: [] },
    })
  })
})
