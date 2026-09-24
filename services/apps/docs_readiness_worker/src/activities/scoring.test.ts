import { afterEach, describe, expect, test, vi } from 'vitest'

import { recordFailure, scoreProject } from './scoring'

const mocks = vi.hoisted(() => ({
  findProjectForDocsDiscovery: vi.fn(),
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
  findProjectForDocsDiscovery: mocks.findProjectForDocsDiscovery,
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
})

describe('recordFailure', () => {
  test('throws a non-retryable failure when the project does not exist', async () => {
    mocks.findProjectForDocsDiscovery.mockResolvedValue(null)
    await expect(recordFailure('missing', 'run-1', RESOLVED, 'no-docs-url')).rejects.toThrow()
    expect(mocks.upsertProjectDocReadiness).not.toHaveBeenCalled()
  })

  test('clears any stale check rows and upserts an ok=false row, in one transaction', async () => {
    mocks.findProjectForDocsDiscovery.mockResolvedValue({
      id: 'project-1',
      slug: 'proj',
      name: 'Project',
      website: null,
    })
    mocks.tx.mockImplementation(async (fn: (tx: unknown) => Promise<void>) => {
      await fn('tx-marker')
    })

    await recordFailure('project-1', 'run-1', RESOLVED, 'no-docs-url')

    expect(mocks.replaceProjectDocReadinessChecks).toHaveBeenCalledWith(
      'tx-marker',
      'project-1',
      [],
    )
    expect(mocks.upsertProjectDocReadiness).toHaveBeenCalledWith(
      'tx-marker',
      expect.objectContaining({
        projectId: 'project-1',
        ok: false,
        error: 'no-docs-url',
        overallScore: null,
        overallGrade: null,
        categoryScores: null,
      }),
    )
  })
})
