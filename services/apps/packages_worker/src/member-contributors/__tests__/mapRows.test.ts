import { describe, expect, it } from 'vitest'

import {
  CdpGovernanceRoleRow,
  canonicalGovernanceRepoUrl,
  classifyIdentity,
  toRepoContributorRow,
} from '../governance/mapRows'

describe('canonicalGovernanceRepoUrl', () => {
  it('lowercases github urls and strips a .git suffix', () => {
    expect(canonicalGovernanceRepoUrl('https://github.com/Kubernetes/Kubernetes.git')).toBe(
      'https://github.com/kubernetes/kubernetes',
    )
  })

  it('preserves case for gitlab urls', () => {
    expect(canonicalGovernanceRepoUrl('https://gitlab.com/Group/Sub/Repo')).toBe(
      'https://gitlab.com/Group/Sub/Repo',
    )
  })

  it('returns null for hosts we do not store', () => {
    expect(
      canonicalGovernanceRepoUrl('https://gerrit.automotivelinux.org/gerrit/AGL/meta-agl'),
    ).toBe(null)
    expect(canonicalGovernanceRepoUrl('git://git.ghostscript.com/mupdf')).toBe(null)
  })

  it('returns null for unparsable values', () => {
    expect(canonicalGovernanceRepoUrl('not a url')).toBe(null)
    expect(canonicalGovernanceRepoUrl('https://github.com/only-owner')).toBe(null)
  })
})

describe('classifyIdentity', () => {
  it('maps github usernames to lowercased github logins', () => {
    expect(classifyIdentity('github', 'username', ' OctoCat ')).toEqual({
      identityType: 'github-login',
      identityValue: 'octocat',
    })
  })

  it('maps git usernames holding an email to lowercased emails', () => {
    expect(classifyIdentity('git', 'username', 'Dev@RedHat.com')).toEqual({
      identityType: 'email',
      identityValue: 'dev@redhat.com',
    })
  })

  it('maps email identities regardless of platform', () => {
    expect(classifyIdentity('gitlab', 'email', 'a@b.io')).toEqual({
      identityType: 'email',
      identityValue: 'a@b.io',
    })
  })

  it('keeps git author names as written', () => {
    expect(classifyIdentity('git', 'username', 'Jane Doe')).toEqual({
      identityType: 'git-author-name',
      identityValue: 'Jane Doe',
    })
  })
})

describe('toRepoContributorRow', () => {
  const syncedAt = new Date('2026-10-02T06:00:00Z')
  const base: CdpGovernanceRoleRow = {
    id: 'cdp-1',
    repoUrl: 'https://github.com/org/repo',
    role: 'maintainer',
    originalRole: 'approver',
    startDate: null,
    endDate: null,
    createdAt: new Date('2026-03-26T21:32:03Z'),
    identityPlatform: 'github',
    identityType: 'username',
    identityValue: 'janedoe',
  }

  it('uses the raw file role, keeps the normalized kind, and falls back dates', () => {
    expect(toRepoContributorRow(base, '42', syncedAt)).toEqual({
      repoId: '42',
      source: 'governance_file',
      role: 'approver',
      roleKind: 'maintainer',
      identityType: 'github-login',
      identityValue: 'janedoe',
      firstSeenAt: base.createdAt,
      lastSeenAt: syncedAt,
      endedAt: null,
    })
  })

  it('prefers the cdp start date and marks ended rows with their end date', () => {
    const startDate = new Date('2026-04-03T00:00:00Z')
    const endDate = new Date('2026-07-31T00:00:00Z')
    const row = toRepoContributorRow(
      { ...base, startDate, endDate, originalRole: null },
      '7',
      syncedAt,
    )
    expect(row.role).toBe('maintainer')
    expect(row.firstSeenAt).toBe(startDate)
    expect(row.lastSeenAt).toBe(endDate)
    expect(row.endedAt).toBe(endDate)
  })
})
