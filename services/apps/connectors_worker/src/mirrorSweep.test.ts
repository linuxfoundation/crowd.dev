import axios from 'axios'
import { describe, expect, it } from 'vitest'

import type { IUnmirroredNangoRepo } from '@crowd/data-access-layer/src/integrations'

import {
  type IMirrorCandidate,
  chunk,
  isRenamedChannel,
  mirrorRoom,
  mirrorSince,
  mirrorSkipReasonFor,
  orderMirrorCandidates,
} from './mirrorSweep'

const repo = (overrides: Partial<IUnmirroredNangoRepo> = {}): IUnmirroredNangoRepo => ({
  integrationId: 'i-1',
  connectionId: 'c-1',
  owner: 'owner',
  repoName: 'repo',
  enabled: true,
  archived: false,
  excluded: false,
  ...overrides,
})

const axiosError = (status: number) =>
  new axios.AxiosError('failed', String(status), undefined, undefined, {
    status,
  } as never)

describe('mirrorRoom', () => {
  it('caps the batch by the remaining shadow room and never goes negative', () => {
    expect(mirrorRoom({ batchSize: 500, shadowCap: 5000 }, 110)).toBe(500)
    expect(mirrorRoom({ batchSize: 2000, shadowCap: 5000 }, 4600)).toBe(400)
    expect(mirrorRoom({ batchSize: 500, shadowCap: 5000 }, 5200)).toBe(0)
  })
})

describe('mirrorSince', () => {
  it('returns midnight UTC of the day 48 hours before now', () => {
    expect(mirrorSince(new Date('2026-10-22T03:00:00Z'))).toBe('2026-10-20T00:00:00.000Z')
  })
})

describe('orderMirrorCandidates', () => {
  it('puts active repos first, then activity desc, then owner and repo name', () => {
    const ordered = orderMirrorCandidates(
      [
        repo({ owner: 'a', repoName: 'archived', archived: true }),
        repo({ owner: 'b', repoName: 'quiet' }),
        repo({ owner: 'c', repoName: 'busy' }),
        repo({ owner: 'z', repoName: 'tie' }),
        repo({ owner: 'y', repoName: 'tie' }),
        repo({ owner: 'y', repoName: 'alpha' }),
      ],
      new Map([
        ['https://github.com/a/archived', 1000],
        ['https://github.com/b/quiet', 10],
        ['https://github.com/c/busy', 50],
        ['https://github.com/z/tie', 5],
        ['https://github.com/y/tie', 5],
        ['https://github.com/y/alpha', 5],
      ]),
    )
    expect(ordered.map((c) => `${c.owner}/${c.repoName}`)).toEqual([
      'c/busy',
      'b/quiet',
      'y/alpha',
      'y/tie',
      'z/tie',
      'a/archived',
    ])
    expect(ordered[0]).toEqual({
      integrationId: 'i-1',
      connectionId: 'c-1',
      owner: 'c',
      repoName: 'busy',
    })
  })

  it('matches activity case-insensitively', () => {
    const ordered = orderMirrorCandidates(
      [repo({ owner: 'A', repoName: 'Low' }), repo({ owner: 'B', repoName: 'High' })],
      new Map([['https://github.com/b/high', 9]]),
    )
    expect(ordered.map((c) => c.repoName)).toEqual(['High', 'Low'])
  })
})

describe('isRenamedChannel', () => {
  it('compares the channel to the mapped owner and repo ignoring case', () => {
    const candidate: IMirrorCandidate = {
      integrationId: 'i-1',
      connectionId: 'c-1',
      owner: 'owner',
      repoName: 'repo',
    }
    expect(isRenamedChannel(candidate, 'https://github.com/Owner/Repo')).toBe(false)
    expect(isRenamedChannel(candidate, 'https://github.com/other/repo')).toBe(true)
  })
})

describe('mirrorSkipReasonFor', () => {
  it('classifies each stage and tells a 404 from other resolve errors', () => {
    expect(mirrorSkipReasonFor('resolve', axiosError(404))).toBe('repo_not_found')
    expect(mirrorSkipReasonFor('resolve', axiosError(500))).toBe('github_error')
    expect(mirrorSkipReasonFor('mint', new Error('x'))).toBe('token_mint_failed')
    expect(mirrorSkipReasonFor('nango', new Error('x'))).toBe('nango_error')
  })
})

describe('chunk', () => {
  it('splits into fixed-size groups and returns nothing for empty input', () => {
    const sizes = chunk(
      Array.from({ length: 250 }, (_, i) => i),
      100,
    ).map((c) => c.length)
    expect(sizes).toEqual([100, 100, 50])
    expect(chunk([], 100)).toEqual([])
  })
})
