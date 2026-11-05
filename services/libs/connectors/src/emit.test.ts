import { describe, expect, it, vi } from 'vitest'
import { z } from 'zod'

import type { DataSinkWorkerEmitter } from '@crowd/common_services'
import type { ISyncUnit } from '@crowd/data-access-layer/src/connectors'
import type { Logger } from '@crowd/logging'
import { IntegrationResultType } from '@crowd/types'

import { createEmit } from './emit'
import { ConnectorCodeError } from './http/errors'

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

    await emitter.emit([record('a')], { segmentId: 'segment-1', integrationId: 'integration-1' })

    expect(deps.publishResult).toHaveBeenCalledWith(
      'integration-1',
      expect.objectContaining({
        type: IntegrationResultType.ACTIVITY,
        segmentId: 'segment-1',
      }),
    )
  })

  it('rejects a partial override (segmentId without integrationId)', async () => {
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

    await expect(emitter.emit([record('a')], { segmentId: 'segment-1' })).rejects.toThrow(
      'segmentId and integrationId overrides must be provided together',
    )
    expect(deps.publishResult).not.toHaveBeenCalled()
  })

  it('rejects a partial override (integrationId without segmentId)', async () => {
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

    await expect(emitter.emit([record('a')], { integrationId: 'integration-1' })).rejects.toThrow(
      'segmentId and integrationId overrides must be provided together',
    )
    expect(deps.publishResult).not.toHaveBeenCalled()
  })

  it('rejects an emit without overrides when the integration has no segment', async () => {
    const deps = makeDeps()
    const emitter = createEmit({
      publishResult: deps.publishResult,
      sinkEmitter: deps.sinkEmitter,
      recordShadow: deps.recordShadow,
      unit: UNIT,
      segmentId: null,
      schema: SCHEMA,
      log: LOG,
    })

    const result = emitter.emit([record('a')])

    await expect(result).rejects.toBeInstanceOf(ConnectorCodeError)
    await expect(result).rejects.toThrow('every emit must pass segmentId and integrationId')
    expect(deps.publishResult).not.toHaveBeenCalled()
    expect(emitter.emittedCount()).toBe(0)
  })

  it('rejects a shadow-mode emit without overrides when the integration has no segment', async () => {
    const deps = makeDeps()
    const emitter = createEmit({
      publishResult: deps.publishResult,
      sinkEmitter: deps.sinkEmitter,
      recordShadow: deps.recordShadow,
      unit: { ...UNIT, emitEnabled: false },
      segmentId: null,
      schema: SCHEMA,
      log: LOG,
    })

    await expect(emitter.emit([record('a')])).rejects.toBeInstanceOf(ConnectorCodeError)
    expect(deps.recordShadow).not.toHaveBeenCalled()
  })

  it('rejects an emit without overrides for an emitsOwnSegment connector on a segmented integration', async () => {
    const deps = makeDeps()
    const emitter = createEmit({
      publishResult: deps.publishResult,
      sinkEmitter: deps.sinkEmitter,
      recordShadow: deps.recordShadow,
      unit: UNIT,
      segmentId: 'segment-default',
      emitsOwnSegment: true,
      schema: SCHEMA,
      log: LOG,
    })

    const result = emitter.emit([record('a')])

    await expect(result).rejects.toBeInstanceOf(ConnectorCodeError)
    await expect(result).rejects.toThrow('connector declares emitsOwnSegment')
    expect(deps.publishResult).not.toHaveBeenCalled()
    expect(deps.recordShadow).not.toHaveBeenCalled()
  })

  it('rejects an empty override value', async () => {
    const deps = makeDeps()
    const emitter = createEmit({
      publishResult: deps.publishResult,
      sinkEmitter: deps.sinkEmitter,
      recordShadow: deps.recordShadow,
      unit: UNIT,
      segmentId: null,
      schema: SCHEMA,
      log: LOG,
    })

    const emptySegment = emitter.emit([record('a')], { segmentId: '', integrationId: 'i-1' })
    const emptyIntegration = emitter.emit([record('a')], { segmentId: 's-1', integrationId: '' })

    await expect(emptySegment).rejects.toBeInstanceOf(ConnectorCodeError)
    await expect(emptySegment).rejects.toThrow('overrides must be non-empty')
    await expect(emptyIntegration).rejects.toThrow('overrides must be non-empty')
    expect(deps.publishResult).not.toHaveBeenCalled()
  })

  it('publishes with the overrides for an emitsOwnSegment connector', async () => {
    const deps = makeDeps()
    const emitter = createEmit({
      publishResult: deps.publishResult,
      sinkEmitter: deps.sinkEmitter,
      recordShadow: deps.recordShadow,
      unit: UNIT,
      segmentId: null,
      emitsOwnSegment: true,
      schema: SCHEMA,
      log: LOG,
    })

    await emitter.emit([record('a')], { segmentId: 'segment-1', integrationId: 'integration-1' })

    expect(deps.published).toEqual([{ integrationId: 'integration-1', segmentId: 'segment-1' }])
  })

  it('publishes with the override values when the integration has no segment', async () => {
    const deps = makeDeps()
    const emitter = createEmit({
      publishResult: deps.publishResult,
      sinkEmitter: deps.sinkEmitter,
      recordShadow: deps.recordShadow,
      unit: UNIT,
      segmentId: null,
      schema: SCHEMA,
      log: LOG,
    })

    await emitter.emit([record('a')], { segmentId: 'segment-1', integrationId: 'integration-1' })

    expect(deps.published).toEqual([{ integrationId: 'integration-1', segmentId: 'segment-1' }])
    expect(emitter.emittedCount()).toBe(1)
  })

  it('persists the overridden segmentId/integrationId on shadow records', async () => {
    const deps = makeDeps()
    const emitter = createEmit({
      publishResult: deps.publishResult,
      sinkEmitter: deps.sinkEmitter,
      recordShadow: deps.recordShadow,
      unit: { ...UNIT, emitEnabled: false },
      segmentId: 'segment-default',
      schema: SCHEMA,
      log: LOG,
    })

    await emitter.emit([record('a')], {
      segmentId: 'segment-1',
      integrationId: 'integration-1',
    })

    expect(deps.recordShadow).toHaveBeenCalledWith([
      expect.objectContaining({
        segmentId: 'segment-1',
        integrationId: 'integration-1',
      }),
    ])
    expect(deps.publishResult).not.toHaveBeenCalled()
  })

  describe('channel', () => {
    const SCHEMA_WITH_CHANNEL = SCHEMA.extend({ channel: z.string().optional() })

    function emitterWith(schema: z.ZodType<Record<string, unknown>>, emitEnabled = true) {
      const deps = makeDeps()
      const emitter = createEmit({
        publishResult: deps.publishResult,
        sinkEmitter: deps.sinkEmitter,
        recordShadow: deps.recordShadow,
        unit: { ...UNIT, emitEnabled },
        segmentId: 'segment-default',
        schema,
        log: LOG,
      })
      const publishedChannels = () =>
        deps.publishResult.mock.calls.map(
          ([, result]) => (result as unknown as { data: { channel: unknown } }).data.channel,
        )
      return { deps, emitter, publishedChannels }
    }

    it('uses the sync unit channel for a record without one (GitHub-style schema)', async () => {
      const { emitter, publishedChannels } = emitterWith(SCHEMA)

      await emitter.emit([record('a')])

      expect(publishedChannels()).toEqual(['octolens-global'])
    })

    it('overwrites a stray channel when the schema does not declare one', async () => {
      const { emitter, publishedChannels } = emitterWith(SCHEMA)

      await emitter.emit([{ ...record('a'), channel: 'r/elsewhere' }])

      expect(publishedChannels()).toEqual(['octolens-global'])
    })

    it('uses the sync unit channel for a passthrough schema record without one (dummy-style)', async () => {
      const { emitter, publishedChannels } = emitterWith(z.record(z.unknown()))

      await emitter.emit([record('a')])

      expect(publishedChannels()).toEqual(['octolens-global'])
    })

    it("keeps a record's own channel when the schema declares one", async () => {
      const { emitter, publishedChannels } = emitterWith(SCHEMA_WITH_CHANNEL)

      await emitter.emit([
        { ...record('a'), channel: 'https://github.com/owner/repo' },
        { ...record('b'), channel: 'kubernetes' },
        record('c'),
        { ...record('d'), channel: '' },
      ])

      expect(publishedChannels()).toEqual([
        'https://github.com/owner/repo',
        'kubernetes',
        'octolens-global',
        'octolens-global',
      ])
    })

    it("keeps a record's own channel on shadow records too", async () => {
      const { deps, emitter } = emitterWith(SCHEMA_WITH_CHANNEL, false)

      await emitter.emit([{ ...record('a'), channel: 'kubernetes' }])

      expect(deps.recordShadow).toHaveBeenCalledWith([
        expect.objectContaining({ data: expect.objectContaining({ channel: 'kubernetes' }) }),
      ])
    })
  })
})
