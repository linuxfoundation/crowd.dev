import { describe, expect, test } from 'vitest'

import {
  PRECHECK_SKIP_REASONS,
  computeExclusivelyLfOwners,
  resolvePrecheckSkipReason,
} from './precheck'

describe('resolvePrecheckSkipReason', () => {
  test('returns null when nothing matches', () => {
    const reason = resolvePrecheckSkipReason(
      { repoUrl: 'https://github.com/some-owner/some-repo' },
      { reposInCdp: new Set(), exclusivelyLfOwners: new Set() },
    )

    expect(reason).toBeNull()
  })

  test('flags a non-GitHub repo', () => {
    const reason = resolvePrecheckSkipReason(
      { repoUrl: 'https://gitlab.com/gitlab-org/gitlab' },
      { reposInCdp: new Set(), exclusivelyLfOwners: new Set() },
    )

    expect(reason).toBe(PRECHECK_SKIP_REASONS.notGithub)
  })

  test('flags a repo already tracked in CDP', () => {
    const reason = resolvePrecheckSkipReason(
      { repoUrl: 'https://github.com/kubernetes/kubernetes' },
      {
        reposInCdp: new Set(['https://github.com/kubernetes/kubernetes']),
        exclusivelyLfOwners: new Set(),
      },
    )

    expect(reason).toBe(PRECHECK_SKIP_REASONS.alreadyInCdp)
  })

  test('flags a repo whose owner is exclusively LF', () => {
    const reason = resolvePrecheckSkipReason(
      { repoUrl: 'https://github.com/kubernetes/new-repo' },
      { reposInCdp: new Set(), exclusivelyLfOwners: new Set(['kubernetes']) },
    )

    expect(reason).toBe(PRECHECK_SKIP_REASONS.lfOwner)
  })

  test('prefers already-in-CDP over the LF-owner criterion', () => {
    const reason = resolvePrecheckSkipReason(
      { repoUrl: 'https://github.com/kubernetes/kubernetes' },
      {
        reposInCdp: new Set(['https://github.com/kubernetes/kubernetes']),
        exclusivelyLfOwners: new Set(['kubernetes']),
      },
    )

    expect(reason).toBe(PRECHECK_SKIP_REASONS.alreadyInCdp)
  })

  test('does not flag a mixed owner even if some repos are LF', () => {
    const reason = resolvePrecheckSkipReason(
      { repoUrl: 'https://github.com/google/new-repo' },
      { reposInCdp: new Set(), exclusivelyLfOwners: new Set() },
    )

    expect(reason).toBeNull()
  })

  test('returns null for a repo URL that fails to canonicalize', () => {
    const reason = resolvePrecheckSkipReason(
      { repoUrl: 'not a url' },
      { reposInCdp: new Set(), exclusivelyLfOwners: new Set() },
    )

    expect(reason).toBeNull()
  })
})

describe('computeExclusivelyLfOwners', () => {
  test('keeps an owner present only in the LF set', () => {
    const result = computeExclusivelyLfOwners(new Set(['kubernetes']), new Set())

    expect(result.has('kubernetes')).toBe(true)
  })

  test('excludes a mixed owner present in both sets', () => {
    const result = computeExclusivelyLfOwners(new Set(['google']), new Set(['google']))

    expect(result.size).toBe(0)
  })
})
