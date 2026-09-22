import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { logAuditFieldChanges, markPypiPackageScanned } from '@crowd/data-access-layer/src/packages'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import { isClientError } from '../../utils/isClientError'
import { ingestOne, ingestPurlsWithGiveUp } from '../activities'
import { fetchProject } from '../fetchProject'
import { INGEST_MAX_ATTEMPTS } from '../retryPolicy'
import { upsertProject } from '../upsertProject'

// Mock the heavy collaborators so we can exercise the pure classification/retry/give-up logic.
vi.mock('../fetchProject', () => ({ fetchProject: vi.fn() }))
vi.mock('../upsertProject', () => ({ upsertProject: vi.fn() }))
vi.mock('@crowd/data-access-layer/src/packages', () => ({
  getUnscannedPypiPurls: vi.fn(),
  logAuditFieldChanges: vi.fn(),
  markPypiPackageScanned: vi.fn(),
}))

const mockFetch = vi.mocked(fetchProject)
const mockUpsert = vi.mocked(upsertProject)
const mockMarkScanned = vi.mocked(markPypiPackageScanned)
const mockAudit = vi.mocked(logAuditFieldChanges)

const qx = {} as QueryExecutor

beforeEach(() => {
  vi.clearAllMocks()
})

// ── isClientError: which (statusCode, kind) combinations are "give up immediately" 4xx ──────
describe('isClientError', () => {
  it('treats NOT_FOUND and any non-429 4xx as a client error', () => {
    expect(isClientError(404, 'NOT_FOUND')).toBe(true)
    expect(isClientError(undefined, 'NOT_FOUND')).toBe(true)
    expect(isClientError(403, 'TRANSIENT')).toBe(true) // 403 leaked in as TRANSIENT kind
    expect(isClientError(400, 'MALFORMED')).toBe(true)
  })

  it('does NOT treat 429 or 5xx/transient as a client error', () => {
    expect(isClientError(429, 'RATE_LIMIT')).toBe(false)
    expect(isClientError(500, 'TRANSIENT')).toBe(false)
    expect(isClientError(undefined, 'TRANSIENT')).toBe(false)
    expect(isClientError(200, 'OK')).toBe(false)
  })
})

// ── ingestOne: success marks scanned; 4xx gives up after retries; transient throws ──────────
describe('ingestOne', () => {
  it('on success upserts and marks the package scanned with the attempt count', async () => {
    mockFetch.mockResolvedValue({ info: { name: 'flask' } } as never)
    mockUpsert.mockResolvedValue({
      purl: 'pkg:pypi/flask',
      changedFields: ['description'],
    } as never)

    await ingestOne(qx, 'pkg:pypi/flask')

    expect(mockAudit).toHaveBeenCalledWith(qx, 'pypi', 'pkg:pypi/flask', ['description'])
    expect(mockMarkScanned).toHaveBeenCalledWith(
      qx,
      'pkg:pypi/flask',
      expect.objectContaining({ status: 'success', attempts: 1 }),
    )
  })

  it('throws on a transient (5xx) result WITHOUT marking the package scanned', async () => {
    mockFetch.mockResolvedValue({
      kind: 'TRANSIENT',
      statusCode: 500,
      message: 'HTTP 500',
    } as never)

    await expect(ingestOne(qx, 'pkg:pypi/flask')).rejects.toThrow()
    expect(mockMarkScanned).not.toHaveBeenCalled()
    expect(mockFetch).toHaveBeenCalledTimes(1)
  })

  it('gives up on a persistent 4xx after the fast retries and marks it scanned(error)', async () => {
    vi.useFakeTimers()
    mockFetch.mockResolvedValue({
      kind: 'NOT_FOUND',
      statusCode: 404,
      message: 'not found',
    } as never)

    const p = ingestOne(qx, 'pkg:pypi/ghost')
    await vi.runAllTimersAsync()
    await p

    expect(mockFetch).toHaveBeenCalledTimes(3)
    expect(mockMarkScanned).toHaveBeenCalledWith(
      qx,
      'pkg:pypi/ghost',
      expect.objectContaining({
        status: 'error',
        attempts: 3,
        httpStatus: 404,
        errorKind: 'NOT_FOUND',
      }),
    )
    vi.useRealTimers()
  })

  it('gives up on a persistent MALFORMED body after retries and marks it scanned(error)', async () => {
    vi.useFakeTimers()
    mockFetch.mockResolvedValue({ kind: 'MALFORMED', message: 'unexpected shape' } as never)

    const p = ingestOne(qx, 'pkg:pypi/weird')
    await vi.runAllTimersAsync()
    await p

    expect(mockFetch).toHaveBeenCalledTimes(3)
    expect(mockMarkScanned).toHaveBeenCalledWith(
      qx,
      'pkg:pypi/weird',
      expect.objectContaining({ status: 'error', attempts: 3, errorKind: 'MALFORMED' }),
    )
    vi.useRealTimers()
  })

  it('throws on a 429/RATE_LIMIT result without marking scanned (rides Temporal retry)', async () => {
    mockFetch.mockResolvedValue({
      kind: 'RATE_LIMIT',
      statusCode: 429,
      message: 'rate limited',
    } as never)

    await expect(ingestOne(qx, 'pkg:pypi/flask')).rejects.toThrow()
    expect(mockMarkScanned).not.toHaveBeenCalled()
    expect(mockFetch).toHaveBeenCalledTimes(1)
  })
})

// ── ingestPurlsWithGiveUp: one persistently-failing package must not stall the cursor ───────
describe('ingestPurlsWithGiveUp', () => {
  const purls = ['pkg:pypi/a', 'pkg:pypi/bad', 'pkg:pypi/c']
  const makeIngest = () =>
    vi.fn((purl: string) =>
      purl === 'pkg:pypi/bad' ? Promise.reject(new Error('transient')) : Promise.resolve(),
    )

  it('rethrows (and aborts the batch) while Temporal retries remain', async () => {
    const ingest = makeIngest()
    await expect(ingestPurlsWithGiveUp(qx, purls, 1, ingest)).rejects.toThrow()
    // aborted at 'bad' — the package after it is never reached, and nothing is given up
    expect(ingest).toHaveBeenCalledTimes(2)
    expect(mockMarkScanned).not.toHaveBeenCalled()
  })

  it('on the final attempt gives up on the bad package and continues past it', async () => {
    const ingest = makeIngest()
    await expect(
      ingestPurlsWithGiveUp(qx, purls, INGEST_MAX_ATTEMPTS, ingest),
    ).resolves.toBeUndefined()
    // every package attempted, including the one after the failure
    expect(ingest).toHaveBeenCalledTimes(3)
    expect(mockMarkScanned).toHaveBeenCalledTimes(1)
    expect(mockMarkScanned).toHaveBeenCalledWith(
      qx,
      'pkg:pypi/bad',
      expect.objectContaining({ status: 'error' }),
    )
  })
})

afterEach(() => {
  vi.useRealTimers()
})
