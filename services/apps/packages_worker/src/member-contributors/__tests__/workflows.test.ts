import { beforeEach, describe, expect, it, vi } from 'vitest'

import type {
  GitActivitySyncCounts,
  GitActivitySyncOptions,
} from '../git-activity/syncGitActivityContributors'
import {
  collectMemberContributors,
  collectMemberContributorsFromInferredRepo,
  resolveIdentityAffiliations,
  syncRepoContributorsFromGitActivity,
} from '../workflows'

const h = vi.hoisted(() => {
  class WorkflowExecutionAlreadyStartedError extends Error {
    constructor(message: string) {
      super(message)
      this.name = 'WorkflowExecutionAlreadyStartedError'
    }
  }
  class ActivityFailure extends Error {
    constructor(
      message: string,
      public readonly cause: Error,
    ) {
      super(message)
      this.name = 'ActivityFailure'
    }
  }
  const acts = {
    syncGitActivityContributors: vi.fn(),
    prepareIdentityAffiliationsRun: vi.fn(),
    resolveIdentityAffiliationsChunk: vi.fn(),
    mergeIdentityAffiliations: vi.fn(),
    startMemberContributorsJob: vi.fn(),
    collectGithubRepoContributors: vi.fn(),
    resolveNewIdentityAffiliations: vi.fn(),
    completeMemberContributorsJob: vi.fn(),
    failMemberContributorsJob: vi.fn(),
    startInferredRepoMemberContributorsJob: vi.fn(),
    resolveRepoViaLlm: vi.fn(),
    fetchGithubRepoContributors: vi.fn(),
    buildInferredRepoMemberContributors: vi.fn(),
    markMemberContributorsJobUnresolvable: vi.fn(),
  }
  const proxyCalls: Array<{ options: Record<string, unknown>; activities: string[] }> = []
  return {
    acts,
    proxyCalls,
    proxyActivities: vi.fn((options: Record<string, unknown>) => {
      const call = { options, activities: [] as string[] }
      proxyCalls.push(call)
      return new Proxy(acts, {
        get(target, name: string) {
          call.activities.push(name)
          return target[name as keyof typeof target]
        },
      })
    }),
    startChild: vi.fn(),
    nonCancellable: vi.fn((fn: () => Promise<unknown>) => fn()),
    continueAsNew: vi.fn(),
    logInfo: vi.fn(),
    WorkflowExecutionAlreadyStartedError,
    ActivityFailure,
  }
})

vi.mock('@temporalio/workflow', () => ({
  proxyActivities: h.proxyActivities,
  startChild: h.startChild,
  continueAsNew: h.continueAsNew,
  workflowInfo: () => ({ startTime: new Date() }),
  ParentClosePolicy: { ABANDON: 'ABANDON' },
  log: { info: h.logInfo },
  WorkflowExecutionAlreadyStartedError: h.WorkflowExecutionAlreadyStartedError,
  ActivityFailure: h.ActivityFailure,
  CancellationScope: { nonCancellable: h.nonCancellable },
}))

const options: GitActivitySyncOptions = { full: false }
const counts: GitActivitySyncCounts = {
  full: false,
  read: 40,
  upserted: 12,
  removed: 2,
  skippedUnknownRepo: 1,
}

beforeEach(() => {
  vi.clearAllMocks()
  h.acts.syncGitActivityContributors.mockResolvedValue(counts)
})

describe('syncRepoContributorsFromGitActivity', () => {
  it('starts the identity affiliations child after the sync and returns the counts', async () => {
    h.startChild.mockResolvedValue({})

    const result = await syncRepoContributorsFromGitActivity(options)

    expect(result).toEqual(counts)
    expect(h.acts.syncGitActivityContributors).toHaveBeenCalledWith(options)
    expect(h.startChild).toHaveBeenCalledTimes(1)
    expect(h.startChild).toHaveBeenCalledWith(resolveIdentityAffiliations, {
      workflowId: 'identity-affiliations-daily',
      taskQueue: 'security-contacts-worker',
      parentClosePolicy: 'ABANDON',
      workflowExecutionTimeout: '6 hours',
    })
    expect(h.acts.syncGitActivityContributors.mock.invocationCallOrder[0]).toBeLessThan(
      h.startChild.mock.invocationCallOrder[0],
    )
  })

  it('logs once and returns the counts when the child is already running', async () => {
    h.startChild.mockRejectedValue(new h.WorkflowExecutionAlreadyStartedError('already started'))

    const result = await syncRepoContributorsFromGitActivity(options)

    expect(result).toEqual(counts)
    expect(h.logInfo).toHaveBeenCalledTimes(1)
  })

  it('propagates any other child start error', async () => {
    h.startChild.mockRejectedValue(new Error('boom'))

    await expect(syncRepoContributorsFromGitActivity(options)).rejects.toThrow('boom')
    expect(h.logInfo).not.toHaveBeenCalled()
  })
})

describe('resolveIdentityAffiliations', () => {
  const zero = { cdp: 0, email_domain: 0, github_company: 0, unresolved: 0 }
  const merged = { inserted: 1, changed: 2, unchanged: 3, removed: 4, byMethod: zero }

  beforeEach(() => {
    h.acts.prepareIdentityAffiliationsRun.mockResolvedValue({
      runStartedAt: '2026-10-13T00:00:00.000Z',
      identities: 7,
    })
    h.acts.mergeIdentityAffiliations.mockResolvedValue(merged)
  })

  it('prepares, pages until a short page, then merges and reports identities', async () => {
    h.acts.resolveIdentityAffiliationsChunk
      .mockResolvedValueOnce({
        lastKey: { identityType: 'email', identityValue: 'a@x.io' },
        read: 5000,
        counts: { ...zero, cdp: 1 },
      })
      .mockResolvedValueOnce({ lastKey: null, read: 0, counts: zero })

    const result = await resolveIdentityAffiliations()

    expect(result).toEqual({ ...merged, identities: 7 })
    expect(h.acts.resolveIdentityAffiliationsChunk).toHaveBeenNthCalledWith(2, {
      runStartedAt: '2026-10-13T00:00:00.000Z',
      afterKey: { identityType: 'email', identityValue: 'a@x.io' },
      limit: 5000,
    })
    expect(h.acts.mergeIdentityAffiliations).toHaveBeenCalledWith({
      runStartedAt: '2026-10-13T00:00:00.000Z',
    })
    expect(h.continueAsNew).not.toHaveBeenCalled()
  })

  it('continues as new with accumulated state after 40 full pages', async () => {
    h.acts.resolveIdentityAffiliationsChunk.mockImplementation(async ({ afterKey }) => ({
      lastKey: { identityType: 'email', identityValue: `${afterKey?.identityValue ?? ''}x` },
      read: 5000,
      counts: { ...zero, email_domain: 1 },
    }))

    await resolveIdentityAffiliations()

    expect(h.acts.resolveIdentityAffiliationsChunk).toHaveBeenCalledTimes(40)
    expect(h.acts.mergeIdentityAffiliations).not.toHaveBeenCalled()
    expect(h.continueAsNew).toHaveBeenCalledWith({
      runStartedAt: '2026-10-13T00:00:00.000Z',
      identities: 7,
      afterKey: { identityType: 'email', identityValue: 'x'.repeat(40) },
      counts: { ...zero, email_domain: 40 },
    })
  })
})

describe('collectMemberContributors', () => {
  const repo = { repoId: '590455', repoUrl: 'https://github.com/a/b' }

  beforeEach(() => {
    h.acts.startMemberContributorsJob.mockResolvedValue(repo)
    h.acts.collectGithubRepoContributors.mockResolvedValue({ status: 'fetched', logins: ['octo'] })
    h.acts.resolveNewIdentityAffiliations.mockResolvedValue({
      identities: 1,
      inserted: 1,
      counts: {},
    })
    h.acts.completeMemberContributorsJob.mockResolvedValue(undefined)
    h.acts.failMemberContributorsJob.mockResolvedValue(undefined)
  })

  it('runs start, collect, resolve, complete in order', async () => {
    await collectMemberContributors({ jobId: 'job-1' })

    expect(h.acts.startMemberContributorsJob).toHaveBeenCalledWith({ jobId: 'job-1' })
    expect(h.acts.collectGithubRepoContributors).toHaveBeenCalledWith(repo)
    expect(h.acts.resolveNewIdentityAffiliations).toHaveBeenCalledWith({ logins: ['octo'] })
    expect(h.acts.completeMemberContributorsJob).toHaveBeenCalledWith({ jobId: 'job-1' })
    expect(h.acts.failMemberContributorsJob).not.toHaveBeenCalled()
    const order = [
      h.acts.startMemberContributorsJob,
      h.acts.collectGithubRepoContributors,
      h.acts.resolveNewIdentityAffiliations,
      h.acts.completeMemberContributorsJob,
    ].map((fn) => fn.mock.invocationCallOrder[0])
    expect([...order].sort((a, b) => a - b)).toEqual(order)
  })

  it('marks the job failed with the activity message and rethrows', async () => {
    h.acts.collectGithubRepoContributors.mockRejectedValue(
      new Error('Repository not found on GitHub'),
    )

    await expect(collectMemberContributors({ jobId: 'job-1' })).rejects.toThrow(
      'Repository not found on GitHub',
    )

    expect(h.acts.failMemberContributorsJob).toHaveBeenCalledWith({
      jobId: 'job-1',
      reason: 'Repository not found on GitHub',
    })
    expect(h.acts.completeMemberContributorsJob).not.toHaveBeenCalled()
    expect(h.nonCancellable).toHaveBeenCalledTimes(1)
  })

  it('unwraps the cause of a wrapped activity failure for the reason', async () => {
    const wrapped = new h.ActivityFailure('Activity task failed', new Error('socket hang up'))
    h.acts.resolveNewIdentityAffiliations.mockRejectedValue(wrapped)

    await expect(collectMemberContributors({ jobId: 'job-1' })).rejects.toBe(wrapped)

    expect(h.acts.failMemberContributorsJob).toHaveBeenCalledWith({
      jobId: 'job-1',
      reason: 'socket hang up',
    })
  })
})

describe('activity proxy options', () => {
  function optionsFor(activity: string): Record<string, unknown> | undefined {
    return h.proxyCalls.find((call) => call.activities.includes(activity))?.options
  }

  it('gives the terminal job activities an unbounded retry policy', () => {
    for (const activity of ['completeMemberContributorsJob', 'failMemberContributorsJob']) {
      const options = optionsFor(activity)
      expect(options?.startToCloseTimeout).toBe('1 minute')
      expect(options?.retry).toEqual({
        initialInterval: '30 seconds',
        backoffCoefficient: 2,
        maximumInterval: '5 minutes',
      })
      expect(options?.retry).not.toHaveProperty('maximumAttempts')
    }
    expect(
      h.proxyCalls.some(
        ({ options }) =>
          (options.retry as { maximumAttempts?: number } | undefined)?.maximumAttempts === 0,
      ),
    ).toBe(false)
  })

  it('lets the GitHub collect activity run for an hour with a five-minute heartbeat', () => {
    expect(optionsFor('collectGithubRepoContributors')).toEqual({
      startToCloseTimeout: '1 hour',
      heartbeatTimeout: '5 minutes',
      retry: { initialInterval: '30 seconds', backoffCoefficient: 2, maximumAttempts: 3 },
    })
    expect(optionsFor('startMemberContributorsJob')).toEqual({
      startToCloseTimeout: '5 minutes',
      retry: { initialInterval: '30 seconds', backoffCoefficient: 2, maximumAttempts: 3 },
    })
  })

  it('bounds the llm resolution to two attempts and retries the inferred repo finisher forever', () => {
    const retry = { initialInterval: '30 seconds', backoffCoefficient: 2, maximumAttempts: 3 }
    expect({
      start: optionsFor('startInferredRepoMemberContributorsJob'),
      resolve: optionsFor('resolveRepoViaLlm'),
      fetch: optionsFor('fetchGithubRepoContributors'),
      build: optionsFor('buildInferredRepoMemberContributors'),
      unresolvable: optionsFor('markMemberContributorsJobUnresolvable'),
    }).toEqual({
      start: { startToCloseTimeout: '5 minutes', retry },
      resolve: { startToCloseTimeout: '3 minutes', retry: { ...retry, maximumAttempts: 2 } },
      fetch: { startToCloseTimeout: '1 hour', heartbeatTimeout: '5 minutes', retry },
      build: { startToCloseTimeout: '5 minutes', retry },
      unresolvable: {
        startToCloseTimeout: '1 minute',
        retry: {
          initialInterval: '30 seconds',
          backoffCoefficient: 2,
          maximumInterval: '5 minutes',
        },
      },
    })
  })
})

describe('collectMemberContributorsFromInferredRepo', () => {
  const job = { jobId: 'job-1' }
  const repoUrl = 'https://github.com/haskell/aeson'
  const body = {
    purl: 'pkg:hackage/aeson',
    name: 'aeson',
    ecosystem: 'hackage',
    repoUrl,
    repoUrlSource: 'llm_inferred',
    repoMappingConfidence: null,
    contributors: [],
    maintainers: { registry: [], governanceFiles: [] },
  }

  beforeEach(() => {
    h.acts.startInferredRepoMemberContributorsJob.mockResolvedValue({
      purl: 'pkg:hackage/aeson',
      ecosystem: 'hackage',
    })
    h.acts.resolveRepoViaLlm.mockResolvedValue({ outcome: 'resolved', repoUrl, confidence: 0.95 })
    h.acts.fetchGithubRepoContributors.mockResolvedValue({ status: 'fetched', logins: ['octo'] })
    h.acts.resolveNewIdentityAffiliations.mockResolvedValue({
      identities: 1,
      inserted: 1,
      counts: {},
    })
    h.acts.buildInferredRepoMemberContributors.mockResolvedValue(body)
  })

  it('resolves the repo, collects and affiliates its contributors, then completes with the body', async () => {
    await expect(collectMemberContributorsFromInferredRepo(job)).resolves.toEqual(body)

    const sequence = [
      h.acts.startInferredRepoMemberContributorsJob,
      h.acts.resolveRepoViaLlm,
      h.acts.fetchGithubRepoContributors,
      h.acts.resolveNewIdentityAffiliations,
      h.acts.buildInferredRepoMemberContributors,
      h.acts.completeMemberContributorsJob,
    ]
    expect(sequence.map((fn) => fn.mock.calls)).toEqual([
      [[job]],
      [[{ jobId: 'job-1', purl: 'pkg:hackage/aeson' }]],
      [[{ repoUrl }]],
      [[{ logins: ['octo'] }]],
      [[{ purl: 'pkg:hackage/aeson', ecosystem: 'hackage', repoUrl, logins: ['octo'] }]],
      [[job]],
    ])
    const order = sequence.map((fn) => fn.mock.invocationCallOrder[0])
    expect(order).toEqual([...order].sort((a, b) => a - b))
    expect(h.acts.failMemberContributorsJob).not.toHaveBeenCalled()
    expect(h.acts.markMemberContributorsJobUnresolvable).not.toHaveBeenCalled()
  })

  it('marks the job unresolvable with the reason and attempted url, returning nothing', async () => {
    h.acts.resolveRepoViaLlm.mockResolvedValue({
      outcome: 'unresolvable',
      reason: 'LLM returned no repository',
      repoUrl: null,
    })

    await expect(collectMemberContributorsFromInferredRepo(job)).resolves.toBeUndefined()

    expect(h.acts.markMemberContributorsJobUnresolvable.mock.calls).toEqual([
      [{ jobId: 'job-1', reason: 'LLM returned no repository', repoUrl: null }],
    ])
    expect(h.acts.fetchGithubRepoContributors).not.toHaveBeenCalled()
    expect(h.acts.completeMemberContributorsJob).not.toHaveBeenCalled()
    expect(h.acts.failMemberContributorsJob).not.toHaveBeenCalled()
  })

  it('fails the job non-cancellably with the activity cause and rethrows', async () => {
    const cause = new Error('Bedrock credentials are not configured')
    h.acts.resolveRepoViaLlm.mockRejectedValue(new h.ActivityFailure('Activity failed', cause))

    await expect(collectMemberContributorsFromInferredRepo(job)).rejects.toThrow('Activity failed')

    expect(h.acts.failMemberContributorsJob.mock.calls).toEqual([
      [{ jobId: 'job-1', reason: 'Bedrock credentials are not configured' }],
    ])
    expect(h.nonCancellable).toHaveBeenCalledTimes(1)
    expect(h.acts.completeMemberContributorsJob).not.toHaveBeenCalled()
  })
})
