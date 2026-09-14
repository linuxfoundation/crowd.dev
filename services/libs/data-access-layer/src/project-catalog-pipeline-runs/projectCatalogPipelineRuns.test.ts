import { test as base, describe, expect } from 'vitest'

import { withQx } from '@crowd/test-kit/db'

import {
  findLatestPipelineRun,
  findPipelineRuns,
  finishPipelineRun,
  startPipelineRun,
} from './projectCatalogPipelineRuns'

const test = withQx(base)

describe('startPipelineRun', () => {
  test('creates a running row with no finishedAt', async ({ qx }) => {
    const run = await startPipelineRun(qx, { stage: 'evaluation' })

    expect(run.stage).toBe('evaluation')
    expect(run.status).toBe('running')
    expect(run.finishedAt).toBeNull()
    expect(run.startedAt).not.toBeNull()
  })
})

describe('finishPipelineRun', () => {
  test('sets finishedAt, elapsedSeconds, and the provided counters', async ({ qx }) => {
    const run = await startPipelineRun(qx, { stage: 'evaluation' })

    const finished = await finishPipelineRun(qx, run.id, {
      status: 'completed',
      totalCandidates: 20,
      succeeded: 18,
      failed: 2,
      evaluator: {
        calls: 20,
        inputTokens: 21713,
        outputTokens: 907,
        costUsd: 0.078753,
        seconds: 20.36,
        models: {
          'us.anthropic.claude-sonnet-4-20250514-v1:0': {
            calls: 20,
            inputTokens: 21713,
            outputTokens: 907,
            costUsd: 0.078753,
          },
        },
      },
    })

    expect(finished).not.toBeNull()
    expect(finished?.status).toBe('completed')
    expect(finished?.finishedAt).not.toBeNull()
    expect(finished?.elapsedSeconds).not.toBeNull()
    expect(finished?.succeeded).toBe(18)
    expect(finished?.failed).toBe(2)
    expect(finished?.evaluatorCalls).toBe(20)
    expect(finished?.evaluatorInputTokens).toBe(21713)
    expect(Number(finished?.evaluatorCostUsd)).toBeCloseTo(0.078753, 6)
    expect(finished?.evaluatorModels).toEqual({
      'us.anthropic.claude-sonnet-4-20250514-v1:0': {
        calls: 20,
        inputTokens: 21713,
        outputTokens: 907,
        costUsd: 0.078753,
      },
    })
  })

  test('is a no-op once a run is already finished', async ({ qx }) => {
    const run = await startPipelineRun(qx, { stage: 'discovery' })
    await finishPipelineRun(qx, run.id, { status: 'completed' })

    const secondAttempt = await finishPipelineRun(qx, run.id, { status: 'failed' })

    expect(secondAttempt).toBeNull()
  })
})

describe('findLatestPipelineRun', () => {
  test('returns the most recently started row for the stage', async ({ qx }) => {
    await startPipelineRun(qx, { stage: 'onboarding' })
    const latest = await startPipelineRun(qx, { stage: 'onboarding' })

    const found = await findLatestPipelineRun(qx, 'onboarding')

    expect(found?.id).toBe(latest.id)
  })
})

describe('findPipelineRuns', () => {
  test('filters by stage', async ({ qx }) => {
    await startPipelineRun(qx, { stage: 'discovery' })
    await startPipelineRun(qx, { stage: 'evaluation' })

    const runs = await findPipelineRuns(qx, { stage: 'evaluation' })

    expect(runs.every((r) => r.stage === 'evaluation')).toBe(true)
    expect(runs.length).toBeGreaterThan(0)
  })
})
