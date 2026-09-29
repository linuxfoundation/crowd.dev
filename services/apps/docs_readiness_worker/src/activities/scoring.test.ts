import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest'

import { recordFailure, scoreProject } from './scoring'

const mocks = vi.hoisted(() => ({
  findLatestProjectDocReadiness: vi.fn(),
  findProjectForDocsDiscovery: vi.fn(),
  lockProjectDocReadiness: vi.fn(),
  replaceProjectDocReadinessChecks: vi.fn(),
  upsertProjectDocReadiness: vi.fn(),
  runChecks: vi.fn(),
  tx: vi.fn(),
}))

vi.mock('../main', () => ({
  svc: {
    postgres: {
      reader: { connection: () => ({}) },
      writer: { connection: () => ({}) },
    },
  },
}))

vi.mock('@crowd/data-access-layer/src/queryExecutor', () => ({
  pgpQx: vi.fn(() => ({ tx: mocks.tx })),
}))

vi.mock('@crowd/data-access-layer', () => ({
  findLatestProjectDocReadiness: mocks.findLatestProjectDocReadiness,
  findProjectForDocsDiscovery: mocks.findProjectForDocsDiscovery,
  lockProjectDocReadiness: mocks.lockProjectDocReadiness,
  replaceProjectDocReadinessChecks: mocks.replaceProjectDocReadinessChecks,
  upsertProjectDocReadiness: mocks.upsertProjectDocReadiness,
}))

vi.mock('../scoring/afdocs', () => ({
  loadAfdocs: vi.fn(async () => ({ runChecks: mocks.runChecks })),
}))

const RESOLVED = {
  docsUrl: 'https://docs.example.com',
  discoveryMethod: 'docs-subdomain' as const,
  confidence: 'high' as const,
  isOverride: false,
}

afterEach(() => {
  vi.clearAllMocks()
})

describe('scoreProject', () => {
  test('throws a non-retryable failure when resolved has no docsUrl', async () => {
    await expect(
      scoreProject('project-1', 'run-1', { ...RESOLVED, docsUrl: null }),
    ).rejects.toThrow()
    expect(mocks.findProjectForDocsDiscovery).not.toHaveBeenCalled()
  })

  test('throws a non-retryable failure when the project does not exist', async () => {
    mocks.findProjectForDocsDiscovery.mockResolvedValue(null)
    await expect(scoreProject('missing', 'run-1', RESOLVED)).rejects.toThrow()
    expect(mocks.runChecks).not.toHaveBeenCalled()
  })

  test('throws a non-retryable failure when docsUrl resolves to a private host', async () => {
    await expect(
      scoreProject('project-1', 'run-1', { ...RESOLVED, docsUrl: 'http://169.254.169.254/' }),
    ).rejects.toThrow()
    expect(mocks.findProjectForDocsDiscovery).not.toHaveBeenCalled()
  })

  test('throws when every check in the report errored instead of persisting a false score', async () => {
    mocks.findProjectForDocsDiscovery.mockResolvedValue({
      id: 'project-1',
      slug: 'proj',
      name: 'Project',
      website: 'https://example.com',
    })
    mocks.runChecks.mockResolvedValue({
      results: [
        {
          id: 'llms-txt-exists',
          category: 'content-discoverability',
          status: 'error',
          message: 'timeout',
        },
        { id: 'redirect-behavior', category: 'url-stability', status: 'error', message: 'timeout' },
      ],
    })

    await expect(scoreProject('project-1', 'run-1', RESOLVED)).rejects.toThrow()
    expect(mocks.upsertProjectDocReadiness).not.toHaveBeenCalled()
  })

  test('rejects if runChecks exceeds the scoring timeout, without persisting anything', async () => {
    vi.useFakeTimers()
    mocks.findProjectForDocsDiscovery.mockResolvedValue({
      id: 'project-1',
      slug: 'proj',
      name: 'Project',
      website: null,
    })
    mocks.runChecks.mockReturnValue(new Promise(() => {}))

    const result = scoreProject('project-1', 'run-1', RESOLVED)
    const assertion = expect(result).rejects.toThrow(/exceeded/)
    await vi.advanceTimersByTimeAsync(25 * 60 * 1000)
    await assertion

    expect(mocks.upsertProjectDocReadiness).not.toHaveBeenCalled()
    vi.useRealTimers()
  })

  test('runs checks, computes scores, and persists both check rows and the readiness row in one transaction', async () => {
    mocks.findProjectForDocsDiscovery.mockResolvedValue({
      id: 'project-1',
      slug: 'proj',
      name: 'Project',
      website: 'https://example.com',
    })
    mocks.runChecks.mockResolvedValue({
      results: [
        { id: 'llms-txt-exists', category: 'content-discoverability', status: 'pass', message: '' },
        { id: 'redirect-behavior', category: 'url-stability', status: 'fail', message: 'bad' },
      ],
    })

    let txCallback: ((tx: unknown) => Promise<void>) | undefined
    mocks.tx.mockImplementation(async (fn: (tx: unknown) => Promise<void>) => {
      txCallback = fn
      await fn('tx-marker')
    })

    await scoreProject('project-1', 'run-1', RESOLVED)

    expect(mocks.runChecks).toHaveBeenCalledWith('https://docs.example.com')
    expect(txCallback).toBeDefined()
    expect(mocks.replaceProjectDocReadinessChecks).toHaveBeenCalledWith(
      'tx-marker',
      'project-1',
      expect.arrayContaining([
        expect.objectContaining({ checkId: 'llms-txt-exists', status: 'pass', durationMs: null }),
        expect.objectContaining({ checkId: 'redirect-behavior', status: 'fail', durationMs: null }),
      ]),
    )
    expect(mocks.upsertProjectDocReadiness).toHaveBeenCalledWith(
      'tx-marker',
      expect.objectContaining({
        projectId: 'project-1',
        projectSlug: 'proj',
        projectName: 'Project',
        runId: 'run-1',
        ok: true,
        error: null,
      }),
    )
  })

  test('takes the project lock on the write transaction before replacing check rows', async () => {
    mocks.findProjectForDocsDiscovery.mockResolvedValue({
      id: 'project-1',
      slug: 'proj',
      name: 'Project',
      website: null,
    })
    mocks.runChecks.mockResolvedValue({
      results: [
        { id: 'llms-txt-exists', category: 'content-discoverability', status: 'pass', message: '' },
      ],
    })
    mocks.tx.mockImplementation(async (fn: (tx: unknown) => Promise<void>) => {
      await fn('tx-marker')
    })

    await scoreProject('project-1', 'run-1', RESOLVED)

    expect(mocks.lockProjectDocReadiness).toHaveBeenCalledWith('tx-marker', 'project-1')
    expect(mocks.lockProjectDocReadiness).toHaveBeenCalledBefore(
      mocks.replaceProjectDocReadinessChecks,
    )
    expect(mocks.lockProjectDocReadiness).toHaveBeenCalledBefore(mocks.upsertProjectDocReadiness)
  })
})

describe('recordFailure', () => {
  test('throws a non-retryable failure when the project does not exist', async () => {
    mocks.findProjectForDocsDiscovery.mockResolvedValue(null)
    await expect(recordFailure('missing', 'run-1', RESOLVED, 'no-docs-url')).rejects.toThrow()
    expect(mocks.upsertProjectDocReadiness).not.toHaveBeenCalled()
  })

  describe('with an existing project', () => {
    beforeEach(() => {
      mocks.findProjectForDocsDiscovery.mockResolvedValue({
        id: 'project-1',
        slug: 'proj',
        name: 'Project',
        website: null,
      })
      mocks.tx.mockImplementation(async (fn: (tx: unknown) => Promise<void>) => {
        await fn('tx-marker')
      })
    })

    const expectFailureRowUpserted = (error: string) =>
      expect(mocks.upsertProjectDocReadiness).toHaveBeenCalledWith(
        'tx-marker',
        expect.objectContaining({
          projectId: 'project-1',
          ok: false,
          error,
          overallScore: null,
          overallGrade: null,
          categoryScores: null,
        }),
      )

    test('takes the project lock on the write transaction before reading or writing', async () => {
      mocks.findLatestProjectDocReadiness.mockResolvedValue({
        docsUrl: 'https://old.example.com',
        ok: true,
      })

      await recordFailure('project-1', 'run-1', RESOLVED, 'timeout')

      expect(mocks.lockProjectDocReadiness).toHaveBeenCalledWith('tx-marker', 'project-1')
      expect(mocks.lockProjectDocReadiness).toHaveBeenCalledBefore(
        mocks.findLatestProjectDocReadiness,
      )
      expect(mocks.lockProjectDocReadiness).toHaveBeenCalledBefore(
        mocks.replaceProjectDocReadinessChecks,
      )
      expect(mocks.lockProjectDocReadiness).toHaveBeenCalledBefore(mocks.upsertProjectDocReadiness)
    })

    test('keeps check rows when the docs URL matches the latest run', async () => {
      mocks.findLatestProjectDocReadiness.mockResolvedValue({
        docsUrl: 'https://docs.example.com/',
        ok: true,
      })

      await recordFailure('project-1', 'run-1', RESOLVED, 'timeout')

      expect(mocks.findLatestProjectDocReadiness).toHaveBeenCalledWith('tx-marker', 'project-1')
      expect(mocks.replaceProjectDocReadinessChecks).not.toHaveBeenCalled()
      expectFailureRowUpserted('timeout')
    })

    test('treats host case and fragment differences as the same URL', async () => {
      mocks.findLatestProjectDocReadiness.mockResolvedValue({
        docsUrl: 'https://Docs.Example.com/#intro',
        ok: true,
      })

      await recordFailure('project-1', 'run-1', RESOLVED, 'timeout')

      expect(mocks.replaceProjectDocReadinessChecks).not.toHaveBeenCalled()
    })

    test('clears check rows when the docs URL changed since the latest run', async () => {
      mocks.findLatestProjectDocReadiness.mockResolvedValue({
        docsUrl: 'https://old.example.com',
        ok: true,
      })

      await recordFailure('project-1', 'run-1', RESOLVED, 'timeout')

      expect(mocks.replaceProjectDocReadinessChecks).toHaveBeenCalledWith(
        'tx-marker',
        'project-1',
        [],
      )
      expectFailureRowUpserted('timeout')
    })

    test('clears check rows when there is no earlier run', async () => {
      mocks.findLatestProjectDocReadiness.mockResolvedValue(null)

      await recordFailure('project-1', 'run-1', RESOLVED, 'timeout')

      expect(mocks.replaceProjectDocReadinessChecks).toHaveBeenCalledWith(
        'tx-marker',
        'project-1',
        [],
      )
      expectFailureRowUpserted('timeout')
    })

    test('keeps check rows across repeated same-URL failures', async () => {
      mocks.findLatestProjectDocReadiness.mockResolvedValue({
        docsUrl: 'https://docs.example.com',
        ok: false,
      })

      await recordFailure('project-1', 'run-1', RESOLVED, 'timeout')

      expect(mocks.replaceProjectDocReadinessChecks).not.toHaveBeenCalled()
      expectFailureRowUpserted('timeout')
    })

    test('clears check rows when the latest row is a failure on a different URL', async () => {
      mocks.findLatestProjectDocReadiness.mockResolvedValue({
        docsUrl: 'https://other.example.com',
        ok: false,
      })

      await recordFailure('project-1', 'run-1', RESOLVED, 'timeout')

      expect(mocks.replaceProjectDocReadinessChecks).toHaveBeenCalledWith(
        'tx-marker',
        'project-1',
        [],
      )
    })

    test('clears check rows when the failed run has no docs URL', async () => {
      mocks.findLatestProjectDocReadiness.mockResolvedValue({
        docsUrl: 'https://docs.example.com',
        ok: true,
      })

      await recordFailure('project-1', 'run-1', { ...RESOLVED, docsUrl: null }, 'no-docs-url')

      expect(mocks.replaceProjectDocReadinessChecks).toHaveBeenCalledWith(
        'tx-marker',
        'project-1',
        [],
      )
      expectFailureRowUpserted('no-docs-url')
    })
  })
})
