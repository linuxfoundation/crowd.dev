import { test as base, describe, expect } from 'vitest'

import { withQx } from '@crowd/test-kit/db'

import {
  findDocReadinessRunById,
  findLatestDocReadinessRun,
  finishDocReadinessRun,
  startDocReadinessRun,
} from './projectDocReadinessRuns'

const test = withQx(base)

describe('startDocReadinessRun', () => {
  test('creates a running row with no finishedAt', async ({ qx }) => {
    const run = await startDocReadinessRun(qx, { trigger: 'scheduled-full', scope: 'lf' })

    expect(run.trigger).toBe('scheduled-full')
    expect(run.scope).toBe('lf')
    expect(run.status).toBe('running')
    expect(run.finishedAt).toBeNull()
    expect(run.startedAt).not.toBeNull()
  })

  test('is idempotent for a retried workflowId', async ({ qx }) => {
    const first = await startDocReadinessRun(qx, {
      trigger: 'on-demand',
      scope: 'lf',
      workflowId: 'wf-1',
    })
    const retried = await startDocReadinessRun(qx, {
      trigger: 'on-demand',
      scope: 'lf',
      workflowId: 'wf-1',
    })

    expect(retried.id).toBe(first.id)
    expect(retried.startedAt).toBe(first.startedAt)
  })

  test('allows many rows with a null workflowId', async ({ qx }) => {
    const a = await startDocReadinessRun(qx, { trigger: 'on-demand', scope: 'lf' })
    const b = await startDocReadinessRun(qx, { trigger: 'on-demand', scope: 'lf' })

    expect(a.id).not.toBe(b.id)
  })
})

describe('finishDocReadinessRun', () => {
  test('sets finishedAt and the provided counters', async ({ qx }) => {
    const run = await startDocReadinessRun(qx, { trigger: 'scheduled-incremental', scope: 'lf' })

    const finished = await finishDocReadinessRun(qx, run.id, {
      status: 'completed',
      totalProjects: 20,
      discovered: 18,
      scored: 17,
      failed: 3,
    })

    expect(finished?.status).toBe('completed')
    expect(finished?.finishedAt).not.toBeNull()
    expect(finished?.totalProjects).toBe(20)
    expect(finished?.discovered).toBe(18)
    expect(finished?.scored).toBe(17)
    expect(finished?.failed).toBe(3)
    expect(finished?.errorMessage).toBeNull()
  })

  test('records the error message on a failed run', async ({ qx }) => {
    const run = await startDocReadinessRun(qx, { trigger: 'on-demand', scope: 'lf' })

    const finished = await finishDocReadinessRun(qx, run.id, {
      status: 'failed',
      errorMessage: 'boom',
    })

    expect(finished?.status).toBe('failed')
    expect(finished?.errorMessage).toBe('boom')
  })

  test('is a no-op once a run is already finished', async ({ qx }) => {
    const run = await startDocReadinessRun(qx, { trigger: 'on-demand', scope: 'lf' })
    await finishDocReadinessRun(qx, run.id, { status: 'completed' })

    const secondAttempt = await finishDocReadinessRun(qx, run.id, { status: 'failed' })

    expect(secondAttempt).toBeNull()
    expect((await findDocReadinessRunById(qx, run.id))?.status).toBe('completed')
  })
})

describe('findLatestDocReadinessRun', () => {
  test('returns the most recently started row, optionally filtered by trigger', async ({ qx }) => {
    await startDocReadinessRun(qx, { trigger: 'scheduled-full', scope: 'lf' })
    const latestIncremental = await startDocReadinessRun(qx, {
      trigger: 'scheduled-incremental',
      scope: 'lf',
    })
    const latestFull = await startDocReadinessRun(qx, { trigger: 'scheduled-full', scope: 'lf' })

    expect((await findLatestDocReadinessRun(qx))?.id).toBe(latestFull.id)
    expect((await findLatestDocReadinessRun(qx, 'scheduled-incremental'))?.id).toBe(
      latestIncremental.id,
    )
  })

  test('returns null when there are no runs', async ({ qx }) => {
    expect(await findLatestDocReadinessRun(qx)).toBeNull()
  })
})
