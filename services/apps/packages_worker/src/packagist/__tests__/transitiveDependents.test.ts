import { beforeEach, describe, expect, it, vi } from 'vitest'

import { EmptyPackagistTransitiveCountsError } from '@crowd/data-access-layer/src/packages/transitiveDependents'

import {
  failPackagistTransitiveRun,
  finishPackagistTransitiveRun,
  mergePackagistTransitiveBatch,
  preparePackagistTransitiveCounts,
} from '../activities'
import {
  ROUNDS_PER_RUN,
  TRANSITIVE_MERGE_BATCH,
  backstopPackagistTransitiveDrain,
  computePackagistTransitiveDependents,
  ingestPackagistMetadata,
} from '../workflows'

// The metadata drain chain-starts the closure workflow on completion (event, not clock).
// The closure runs prepare → keyset merge drain → finish across continueAsNew generations.

const h = vi.hoisted(() => ({
  acts: {
    packagistCurrentTimestamp: vi.fn(),
    packagistStopAfterFirstPage: vi.fn(),
    getPackagistMetadataBatch: vi.fn(),
    ingestPackagistMetadataBatch: vi.fn(),
    preparePackagistTransitiveCounts: vi.fn(),
    mergePackagistTransitiveBatch: vi.fn(),
    finishPackagistTransitiveRun: vi.fn(),
    failPackagistTransitiveRun: vi.fn(),
    packagistTransitiveRanRecently: vi.fn(),
  },
  startChild: vi.fn(),
  continueAsNew: vi.fn(),
  logWarn: vi.fn(),
  attempt: vi.fn(),
  snapshot: vi.fn(),
  closure: vi.fn(),
  mergeDal: vi.fn(),
  createRun: vi.fn(),
  findPendingRun: vi.fn(),
  markMerging: vi.fn(),
  finishRun: vi.fn(),
  failRun: vi.fn(),
  fakeQx: {
    select: vi.fn(),
    selectOne: vi.fn(),
    selectOneOrNone: vi.fn(),
    result: vi.fn(),
    tx: vi.fn(),
  },
}))

vi.mock('@temporalio/workflow', () => ({
  proxyActivities: () => h.acts,
  startChild: h.startChild,
  continueAsNew: h.continueAsNew,
  log: { info: vi.fn(), warn: h.logWarn, error: vi.fn(), debug: vi.fn() },
  ParentClosePolicy: { ABANDON: 'ABANDON' },
  WorkflowIdReusePolicy: { ALLOW_DUPLICATE: 'ALLOW_DUPLICATE' },
}))

vi.mock('@temporalio/activity', async (importOriginal) => ({
  ...(await importOriginal<object>()),
  Context: {
    current: () => ({ info: { attempt: h.attempt() }, heartbeat: vi.fn() }),
  },
}))

vi.mock('../../db', async (importOriginal) => ({
  ...(await importOriginal<object>()),
  getPackagesDb: async () => h.fakeQx,
}))

vi.mock('@crowd/data-access-layer/src/packages/transitiveDependents', async (importOriginal) => ({
  ...(await importOriginal<object>()),
  snapshotPackagistDirectEdges: h.snapshot,
  computePackagistTransitiveCounts: h.closure,
  mergePackagistTransitiveCounts: h.mergeDal,
}))

vi.mock(
  '@crowd/data-access-layer/src/packages/packagistTransitiveRuns',
  async (importOriginal) => ({
    ...(await importOriginal<object>()),
    createPackagistTransitiveRun: h.createRun,
    findUnfinishedPackagistTransitiveRun: h.findPendingRun,
    markPackagistTransitiveRunMerging: h.markMerging,
    finishPackagistTransitiveRun: h.finishRun,
    failPackagistTransitiveRun: h.failRun,
  }),
)

function metadataCandidates(n: number) {
  return Array.from({ length: n }, (_, i) => ({
    purl: `pkg:composer/t/p${i}`,
    metadataLastModified: null,
  }))
}

beforeEach(() => {
  vi.clearAllMocks()
  h.acts.packagistCurrentTimestamp.mockResolvedValue('2026-07-26T00:00:00.000Z')
  h.acts.packagistStopAfterFirstPage.mockResolvedValue(false)
  h.acts.ingestPackagistMetadataBatch.mockResolvedValue(undefined)
  h.acts.finishPackagistTransitiveRun.mockResolvedValue(undefined)
  h.acts.failPackagistTransitiveRun.mockResolvedValue(undefined)
  h.startChild.mockResolvedValue(undefined)
  h.continueAsNew.mockResolvedValue(undefined)
  h.attempt.mockReturnValue(1)
})

describe('ingestPackagistMetadata — chaining the transitive drain', () => {
  it('chain-starts the transitive drain when the drain completes on an empty batch', async () => {
    h.acts.getPackagistMetadataBatch.mockResolvedValue({ candidates: [], nextCursor: '' })

    await ingestPackagistMetadata({})

    expect(h.acts.ingestPackagistMetadataBatch).not.toHaveBeenCalled()
    expect(h.startChild).toHaveBeenCalledTimes(1)
    const [wf, opts] = h.startChild.mock.calls[0]
    expect(wf).toBe(computePackagistTransitiveDependents)
    expect(opts).toMatchObject({
      workflowId: 'packagist-transitive-drain',
      workflowIdReusePolicy: 'ALLOW_DUPLICATE',
      parentClosePolicy: 'ABANDON',
      args: [{}],
    })
  })

  it('chain-starts after ingesting the final short batch', async () => {
    h.acts.getPackagistMetadataBatch.mockResolvedValue({
      candidates: metadataCandidates(3),
      nextCursor: 'pkg:composer/t/p2',
    })

    await ingestPackagistMetadata({})

    expect(h.acts.ingestPackagistMetadataBatch).toHaveBeenCalledTimes(1)
    expect(h.startChild).toHaveBeenCalledTimes(1)
    // the final batch is fully ingested before the drain is chained
    expect(h.acts.ingestPackagistMetadataBatch.mock.invocationCallOrder[0]).toBeLessThan(
      h.startChild.mock.invocationCallOrder[0],
    )
  })

  it('does not chain in stopAfterFirstPage debug mode', async () => {
    h.acts.packagistStopAfterFirstPage.mockResolvedValue(true)
    h.acts.getPackagistMetadataBatch.mockResolvedValue({
      candidates: metadataCandidates(50),
      nextCursor: 'pkg:composer/t/p49',
    })

    await ingestPackagistMetadata({})

    expect(h.acts.ingestPackagistMetadataBatch).toHaveBeenCalledTimes(1)
    expect(h.startChild).not.toHaveBeenCalled()
  })

  it('does not chain on an empty batch in stopAfterFirstPage debug mode either', async () => {
    h.acts.packagistStopAfterFirstPage.mockResolvedValue(true)
    h.acts.getPackagistMetadataBatch.mockResolvedValue({ candidates: [], nextCursor: '' })

    await ingestPackagistMetadata({})

    expect(h.startChild).not.toHaveBeenCalled()
  })

  it('does not chain while the drain still has full batches — it continues-as-new instead', async () => {
    h.acts.getPackagistMetadataBatch.mockResolvedValue({
      candidates: metadataCandidates(50),
      nextCursor: 'pkg:composer/t/p49',
    })

    await ingestPackagistMetadata({})

    expect(h.continueAsNew).toHaveBeenCalledTimes(1)
    expect(h.startChild).not.toHaveBeenCalled()
  })

  it('swallows already-started when a prior transitive drain is still running', async () => {
    h.acts.getPackagistMetadataBatch.mockResolvedValue({ candidates: [], nextCursor: '' })
    h.startChild.mockRejectedValue(
      Object.assign(new Error('already started'), {
        name: 'WorkflowExecutionAlreadyStartedError',
      }),
    )

    await expect(ingestPackagistMetadata({})).resolves.toBeUndefined()
    expect(h.logWarn).toHaveBeenCalled()
  })
})

describe('computePackagistTransitiveDependents — workflow', () => {
  it('prepares once, drains merge batches, then finishes the job with totals', async () => {
    h.acts.preparePackagistTransitiveCounts.mockResolvedValue({ runId: 7 })
    h.acts.mergePackagistTransitiveBatch
      .mockResolvedValueOnce({ processed: TRANSITIVE_MERGE_BATCH, changed: 5, nextCursor: '100' })
      .mockResolvedValueOnce({ processed: 20, changed: 3, nextCursor: '120' })

    await computePackagistTransitiveDependents({})

    expect(h.acts.preparePackagistTransitiveCounts).toHaveBeenCalledTimes(1)
    expect(h.acts.mergePackagistTransitiveBatch).toHaveBeenNthCalledWith(
      1,
      '',
      TRANSITIVE_MERGE_BATCH,
    )
    expect(h.acts.mergePackagistTransitiveBatch).toHaveBeenNthCalledWith(
      2,
      '100',
      TRANSITIVE_MERGE_BATCH,
    )
    expect(h.acts.finishPackagistTransitiveRun).toHaveBeenCalledWith(7, {
      processed: TRANSITIVE_MERGE_BATCH + 20,
      changed: 8,
    })
    expect(h.continueAsNew).not.toHaveBeenCalled()
  })

  it('finishes immediately when the first batch is already empty', async () => {
    h.acts.preparePackagistTransitiveCounts.mockResolvedValue({ runId: 7 })
    h.acts.mergePackagistTransitiveBatch.mockResolvedValue({
      processed: 0,
      changed: 0,
      nextCursor: '',
    })

    await computePackagistTransitiveDependents({})

    expect(h.acts.finishPackagistTransitiveRun).toHaveBeenCalledWith(7, {
      processed: 0,
      changed: 0,
    })
  })

  it('skips prepare when resuming a later generation and accumulates totals', async () => {
    h.acts.mergePackagistTransitiveBatch.mockResolvedValue({
      processed: 20,
      changed: 2,
      nextCursor: '220',
    })

    await computePackagistTransitiveDependents({
      runId: 7,
      cursor: '200',
      processed: 40,
      changed: 6,
    })

    expect(h.acts.preparePackagistTransitiveCounts).not.toHaveBeenCalled()
    expect(h.acts.mergePackagistTransitiveBatch).toHaveBeenCalledWith('200', TRANSITIVE_MERGE_BATCH)
    expect(h.acts.finishPackagistTransitiveRun).toHaveBeenCalledWith(7, {
      processed: 60,
      changed: 8,
    })
  })

  it('continues-as-new with carried state when the round cap is hit', async () => {
    h.acts.preparePackagistTransitiveCounts.mockResolvedValue({ runId: 7 })
    let call = 0
    h.acts.mergePackagistTransitiveBatch.mockImplementation(async () => {
      call += 1
      return { processed: TRANSITIVE_MERGE_BATCH, changed: 1, nextCursor: String(call * 10) }
    })

    await computePackagistTransitiveDependents({})

    expect(h.acts.finishPackagistTransitiveRun).not.toHaveBeenCalled()
    // Pin the cap itself — deriving expectations from the observed call count would
    // stay green for any (broken) number of rounds.
    expect(h.acts.mergePackagistTransitiveBatch).toHaveBeenCalledTimes(ROUNDS_PER_RUN)
    expect(h.continueAsNew).toHaveBeenCalledTimes(1)
    expect(h.continueAsNew).toHaveBeenCalledWith({
      runId: 7,
      cursor: String(ROUNDS_PER_RUN * 10),
      processed: ROUNDS_PER_RUN * TRANSITIVE_MERGE_BATCH,
      changed: ROUNDS_PER_RUN,
    })
  })

  it('marks the run failed with the ROOT cause and rethrows when a merge batch fails permanently', async () => {
    h.acts.preparePackagistTransitiveCounts.mockResolvedValue({ runId: 7 })
    // Shaped like Temporal's ActivityFailure: a generic wrapper whose cause chain
    // carries the real reason — error_message must record the root, not the wrapper.
    h.acts.mergePackagistTransitiveBatch.mockRejectedValue(
      Object.assign(new Error('Activity task failed'), {
        cause: new Error('counts table is empty'),
      }),
    )

    await expect(computePackagistTransitiveDependents({})).rejects.toThrow(/Activity task failed/)

    expect(h.acts.failPackagistTransitiveRun).toHaveBeenCalledWith(7, 'counts table is empty')
    expect(h.acts.finishPackagistTransitiveRun).not.toHaveBeenCalled()
    expect(h.continueAsNew).not.toHaveBeenCalled()
  })

  it('rethrows the ORIGINAL merge error even when fail-marking itself fails', async () => {
    h.acts.preparePackagistTransitiveCounts.mockResolvedValue({ runId: 7 })
    h.acts.mergePackagistTransitiveBatch.mockRejectedValue(new Error('merge exploded'))
    h.acts.failPackagistTransitiveRun.mockRejectedValueOnce(new Error('ledger write refused'))

    await expect(computePackagistTransitiveDependents({})).rejects.toThrow(/merge exploded/)
  })
})

describe('backstopPackagistTransitiveDrain — workflow', () => {
  it('does nothing when a run completed recently', async () => {
    h.acts.packagistTransitiveRanRecently.mockResolvedValue(true)

    await backstopPackagistTransitiveDrain()

    expect(h.startChild).not.toHaveBeenCalled()
  })

  it('chain-starts the drain (fixed workflow id) when the week had no successful run', async () => {
    h.acts.packagistTransitiveRanRecently.mockResolvedValue(false)

    await backstopPackagistTransitiveDrain()

    expect(h.startChild).toHaveBeenCalledTimes(1)
    const [wf, opts] = h.startChild.mock.calls[0]
    expect(wf).toBe(computePackagistTransitiveDependents)
    expect(opts).toMatchObject({ workflowId: 'packagist-transitive-drain' })
  })

  it('swallows already-started so it can never race a live drain', async () => {
    h.acts.packagistTransitiveRanRecently.mockResolvedValue(false)
    h.startChild.mockRejectedValueOnce(
      Object.assign(new Error('already started'), {
        name: 'WorkflowExecutionAlreadyStartedError',
      }),
    )

    await expect(backstopPackagistTransitiveDrain()).resolves.toBeUndefined()
  })
})

describe('preparePackagistTransitiveCounts — activity', () => {
  it('creates a run, snapshots edges, computes the closure, and marks the run merging', async () => {
    h.findPendingRun.mockResolvedValue(null)
    h.createRun.mockResolvedValue(42)
    h.snapshot.mockResolvedValue(918346)
    h.closure.mockResolvedValue(85600)

    const result = await preparePackagistTransitiveCounts()

    expect(h.createRun).toHaveBeenCalledWith(h.fakeQx)
    expect(result).toEqual({ runId: 42 })
    expect(h.closure).toHaveBeenCalledWith(h.fakeQx)
    expect(h.markMerging).toHaveBeenCalledWith(h.fakeQx, 42, {
      edgeCount: 918346,
      packagesWithDependents: 85600,
    })
  })

  it('adopts an unfinished run row from a prior attempt (even one already merging)', async () => {
    h.findPendingRun.mockResolvedValue(41)
    h.snapshot.mockResolvedValue(10)
    h.closure.mockResolvedValue(4)

    const result = await preparePackagistTransitiveCounts()

    expect(h.createRun).not.toHaveBeenCalled()
    expect(result.runId).toBe(41)
  })

  it('does not fail-mark the run on a retryable error before the final attempt', async () => {
    h.findPendingRun.mockResolvedValue(null)
    h.createRun.mockResolvedValue(44)
    h.snapshot.mockRejectedValue(new Error('connection reset'))
    h.attempt.mockReturnValue(1)

    await expect(preparePackagistTransitiveCounts()).rejects.toThrow(/connection reset/)

    // the retry adopts the same unfinished row — fail-marking it early would strand it
    expect(h.failRun).not.toHaveBeenCalled()
  })

  it('fail-marks the run when a retryable error exhausts the final attempt', async () => {
    h.findPendingRun.mockResolvedValue(44)
    h.snapshot.mockRejectedValue(new Error('connection reset'))
    h.attempt.mockReturnValue(3)

    await expect(preparePackagistTransitiveCounts()).rejects.toThrow(/connection reset/)

    expect(h.failRun).toHaveBeenCalledWith(h.fakeQx, 44, 'connection reset')
  })

  it('aborts and marks the run failed when the edge snapshot is empty', async () => {
    h.findPendingRun.mockResolvedValue(null)
    h.createRun.mockResolvedValue(43)
    h.snapshot.mockResolvedValue(0)

    await expect(preparePackagistTransitiveCounts()).rejects.toThrow(/no packagist direct edges/i)

    expect(h.closure).not.toHaveBeenCalled()
    expect(h.failRun).toHaveBeenCalledWith(
      h.fakeQx,
      43,
      expect.stringMatching(/no packagist direct edges/i),
    )
  })

  it('rethrows the ORIGINAL non-retryable abort even when fail-marking itself fails', async () => {
    h.findPendingRun.mockResolvedValue(null)
    h.createRun.mockResolvedValue(43)
    h.snapshot.mockResolvedValue(0)
    h.failRun.mockRejectedValueOnce(new Error('ledger write refused'))

    // A masked original would surface as the retryable ledger error and let Temporal
    // rerun the full package_dependencies scan.
    await expect(preparePackagistTransitiveCounts()).rejects.toThrow(/no packagist direct edges/i)
  })
})

describe('finish/fail run — activities', () => {
  it('marks the run done with the drain totals', async () => {
    await finishPackagistTransitiveRun(7, { processed: 454000, changed: 86000 })

    expect(h.finishRun).toHaveBeenCalledWith(h.fakeQx, 7, { processed: 454000, changed: 86000 })
  })

  it('marks the run failed with the error message', async () => {
    await failPackagistTransitiveRun(7, 'merge exploded')

    expect(h.failRun).toHaveBeenCalledWith(h.fakeQx, 7, 'merge exploded')
  })
})

describe('mergePackagistTransitiveBatch — activity', () => {
  it('classifies an empty counts table as non-retryable', async () => {
    h.mergeDal.mockRejectedValue(new EmptyPackagistTransitiveCountsError())

    await expect(mergePackagistTransitiveBatch('', 10)).rejects.toMatchObject({
      nonRetryable: true,
    })
  })

  it('lets other merge errors stay retryable', async () => {
    h.mergeDal.mockRejectedValue(new Error('deadlock detected'))

    await expect(mergePackagistTransitiveBatch('', 10)).rejects.toSatisfy(
      (err: unknown) => err instanceof Error && !('nonRetryable' in err && err.nonRetryable),
    )
  })
})
