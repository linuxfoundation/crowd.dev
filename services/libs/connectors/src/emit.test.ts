import { describe, expect, it, vi } from 'vitest'
import { z } from 'zod'

import type { DataSinkWorkerEmitter } from '@crowd/common_services'
import type { ISyncUnit } from '@crowd/data-access-layer/src/connectors'
import type { Logger } from '@crowd/logging'
import { IntegrationResultType } from '@crowd/types'

import { createEmit } from './emit'

const SCHEMA = z.object({
  type: z.string(),
  sourceId: z.string(),
  timestamp: z.string(),
})

const UNIT: ISyncUnit = {
  id: 'unit-1',
  integrationId: 'integration-default',
  platform: 'octolens',
  channelId: 'channel-1',
  channelName: 'octolens-global',
  syncName: 'mentionPoll',
  status: 'active',
  nextRunAt: new Date().toISOString(),
  lockedAt: null,
  lastRunAt: null,
  lastSuccessAt: null,
  consecutiveFailures: 0,
  lastErrorClass: null,
  lastErrorMessage: null,
  lastRunComplete: null,
  watermark: null,
  emittedCount: null,
  emitEnabled: true,
}

const LOG = {
  debug: vi.fn(),
  info: vi.fn(),
  warn: vi.fn(),
  error: vi.fn(),
} as unknown as Logger

function makeDeps() {
  const published: { integrationId: string; segmentId?: string }[] = []
  const publishResult = vi.fn(async (integrationId: string, result: { segmentId?: string }) => {
    published.push({ integrationId, segmentId: result.segmentId })
    return `result-${published.length}`
  })
  const triggerResultProcessing = vi.fn(async () => undefined)
  const sinkEmitter = { triggerResultProcessing } as unknown as DataSinkWorkerEmitter
  const recordShadow = vi.fn(async () => undefined)

  return {
    published,
    publishResult,
    sinkEmitter,
    recordShadow,
    triggerResultProcessing,
  }
}

function record(sourceId: string) {
  return { type: 'mention', sourceId, timestamp: '2026-10-01T00:00:00Z' }
}

describe('createEmit', () => {
  it('uses the fixed deps segmentId/integrationId when no override is passed', async () => {
    const deps = makeDeps()
    const emitter = createEmit({
      publishResult: deps.publishResult,
      sinkEmitter: deps.sinkEmitter,
      recordShadow: deps.recordShadow,
      unit: UNIT,
      segmentId: 'segment-default',
      schema: SCHEMA,
      log: LOG,
    })

    await emitter.emit([record('a')])

    expect(deps.published).toEqual([
      { integrationId: 'integration-default', segmentId: 'segment-default' },
    ])
    expect(emitter.emittedCount()).toBe(1)
  })

  it('fans out two emit() calls in one run to different segments/integrations', async () => {
    const deps = makeDeps()
    const emitter = createEmit({
      publishResult: deps.publishResult,
      sinkEmitter: deps.sinkEmitter,
      recordShadow: deps.recordShadow,
      unit: UNIT,
      segmentId: 'segment-default',
      schema: SCHEMA,
      log: LOG,
    })

    await emitter.emit([record('a')], {
      segmentId: 'segment-1',
      integrationId: 'integration-1',
    })
    await emitter.emit([record('b')], {
      segmentId: 'segment-2',
      integrationId: 'integration-2',
    })

    expect(deps.published).toEqual([
      { integrationId: 'integration-1', segmentId: 'segment-1' },
      { integrationId: 'integration-2', segmentId: 'segment-2' },
    ])
    expect(emitter.emittedCount()).toBe(2)
  })

  it('passes the correct result type and overridden segmentId through to publishResult', async () => {
    const deps = makeDeps()
    const emitter = createEmit({
      publishResult: deps.publishResult,
      sinkEmitter: deps.sinkEmitter,
      recordShadow: deps.recordShadow,
      unit: UNIT,
      segmentId: 'segment-default',
      schema: SCHEMA,
      log: LOG,
    })

    await emitter.emit([record('a')], { segmentId: 'segment-1' })

    expect(deps.publishResult).toHaveBeenCalledWith(
      'integration-default',
      expect.objectContaining({
        type: IntegrationResultType.ACTIVITY,
        segmentId: 'segment-1',
      }),
    )
  })
})
