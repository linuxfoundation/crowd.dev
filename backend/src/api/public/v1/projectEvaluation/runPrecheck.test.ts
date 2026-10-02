import { beforeEach, describe, expect, it, vi } from 'vitest'

import {
  findGithubOwnersWithLfProjects,
  findGithubOwnersWithNonLfRepos,
  findRepoUrlsInCdp,
} from '@crowd/data-access-layer'
import { PRECHECK_SKIP_REASONS } from '@crowd/data-access-layer/src/project-catalog/precheck'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import { precheckSkipResult, runPrecheck } from './runPrecheck'

vi.mock('@crowd/data-access-layer', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@crowd/data-access-layer')>()),
  findRepoUrlsInCdp: vi.fn(),
  findGithubOwnersWithLfProjects: vi.fn(),
  findGithubOwnersWithNonLfRepos: vi.fn(),
}))

const qx = {} as QueryExecutor

describe('runPrecheck', () => {
  beforeEach(() => {
    vi.mocked(findRepoUrlsInCdp).mockReset().mockResolvedValue(new Set())
    vi.mocked(findGithubOwnersWithLfProjects).mockReset().mockResolvedValue(new Set())
    vi.mocked(findGithubOwnersWithNonLfRepos).mockReset().mockResolvedValue(new Set())
  })

  it('skips a repo that is already tracked in CDP, querying with the canonical URL', async () => {
    vi.mocked(findRepoUrlsInCdp).mockResolvedValue(new Set(['https://github.com/foo/bar']))

    await expect(runPrecheck(qx, 'https://GitHub.com/Foo/Bar.git')).resolves.toBe(
      PRECHECK_SKIP_REASONS.alreadyInCdp,
    )
    expect(findRepoUrlsInCdp).toHaveBeenCalledWith(qx, ['https://github.com/foo/bar'])
  })

  it('skips a repo whose owner is exclusively mapped to LF projects', async () => {
    vi.mocked(findGithubOwnersWithLfProjects).mockResolvedValue(new Set(['foo']))

    await expect(runPrecheck(qx, 'https://github.com/foo/bar')).resolves.toBe(
      PRECHECK_SKIP_REASONS.lfOwner,
    )
  })

  it('does not skip when the owner also has non-LF repos', async () => {
    vi.mocked(findGithubOwnersWithLfProjects).mockResolvedValue(new Set(['foo']))
    vi.mocked(findGithubOwnersWithNonLfRepos).mockResolvedValue(new Set(['foo']))

    await expect(runPrecheck(qx, 'https://github.com/foo/bar')).resolves.toBeNull()
  })

  it('skips a non-GitHub repo without querying for repos or owners', async () => {
    await expect(runPrecheck(qx, 'https://gitlab.com/foo/bar')).resolves.toBe(
      PRECHECK_SKIP_REASONS.notGithub,
    )
    expect(findRepoUrlsInCdp).toHaveBeenCalledWith(qx, [])
    expect(findGithubOwnersWithLfProjects).toHaveBeenCalledWith(qx, [])
    expect(findGithubOwnersWithNonLfRepos).toHaveBeenCalledWith(qx, [])
  })

  it('returns null when nothing matches', async () => {
    await expect(runPrecheck(qx, 'https://github.com/foo/bar')).resolves.toBeNull()
  })
})

describe('precheckSkipResult', () => {
  it('builds a skip response with the reason and no LLM metrics', () => {
    expect(precheckSkipResult(PRECHECK_SKIP_REASONS.alreadyInCdp)).toEqual({
      outcome: 'skip',
      evaluationResult: 'false',
      evaluationReason: PRECHECK_SKIP_REASONS.alreadyInCdp,
      metrics: null,
    })
  })
})
