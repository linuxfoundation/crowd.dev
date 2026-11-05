import { ZodError, ZodType } from 'zod'

import type { DataSinkWorkerEmitter } from '@crowd/common_services'
import type { IShadowRecord, ISyncUnit } from '@crowd/data-access-layer/src/connectors'
import type { Logger } from '@crowd/logging'
import { IIntegrationResult, IntegrationResultType } from '@crowd/types'

import { ConnectorCodeError, ConnectorError } from './http/errors'

export interface EmitOverrides {
  segmentId?: string
  integrationId?: string
}

export interface EmitterDeps {
  publishResult: (integrationId: string, result: IIntegrationResult) => Promise<string>
  sinkEmitter: DataSinkWorkerEmitter
  recordShadow: (records: IShadowRecord[]) => Promise<void>
  unit: ISyncUnit
  segmentId: string | null
  emitsOwnSegment?: boolean
  schema: ZodType<Record<string, unknown>>
  log: Logger
}

export interface Emitter {
  emit: (records: unknown[], overrides?: EmitOverrides) => Promise<void>
  emittedCount: () => number
}

export function createEmit(deps: EmitterDeps): Emitter {
  let emitted = 0

  const emit = async (records: unknown[], overrides?: EmitOverrides): Promise<void> => {
    if (
      overrides &&
      (overrides.segmentId === undefined) !== (overrides.integrationId === undefined)
    ) {
      throw new ConnectorCodeError(
        'segmentId and integrationId overrides must be provided together',
      )
    }

    const hasOverrides = overrides?.segmentId !== undefined
    if (hasOverrides && (!overrides.segmentId || !overrides.integrationId)) {
      throw new ConnectorCodeError('segmentId and integrationId overrides must be non-empty')
    }
    if (!hasOverrides && deps.emitsOwnSegment) {
      throw new ConnectorCodeError(
        'connector declares emitsOwnSegment, so every emit must pass segmentId and integrationId overrides',
      )
    }

    const segmentId = overrides?.segmentId ?? deps.segmentId
    if (!segmentId) {
      throw new ConnectorCodeError(
        'integration has no segment, so every emit must pass segmentId and integrationId overrides',
      )
    }
    const integrationId = overrides?.integrationId ?? deps.unit.integrationId
    const shadowRecords: IShadowRecord[] = []

    for (const record of records) {
      let parsed: Record<string, unknown>
      try {
        parsed = deps.schema.parse(record)
      } catch (err) {
        if (err instanceof ZodError) {
          throw new ConnectorCodeError('record failed schema validation', {
            cause: err,
          })
        }
        throw err
      }

      const payload = { ...parsed, channel: recordChannel(parsed) ?? deps.unit.channelName }

      if (!deps.unit.emitEnabled) {
        const { type, sourceId, timestamp } = payload as Record<string, unknown>
        if (typeof type !== 'string' || typeof sourceId !== 'string' || !timestamp) {
          throw new ConnectorCodeError(
            'record missing type/sourceId/timestamp required for shadow mode',
          )
        }
        shadowRecords.push({
          type,
          sourceId,
          occurredAt: String(timestamp),
          data: payload,
          segmentId,
          integrationId,
        })
        emitted += 1
        continue
      }

      try {
        const resultId = await deps.publishResult(integrationId, {
          type: IntegrationResultType.ACTIVITY,
          segmentId,
          data: payload,
        })
        await deps.sinkEmitter.triggerResultProcessing(resultId, resultId, false)
      } catch (err) {
        if (err instanceof ConnectorError) {
          throw err
        }
        throw new ConnectorError('sink.rejected', 'failed to hand record to sink', { cause: err })
      }

      emitted += 1
    }

    if (shadowRecords.length > 0) {
      await deps.recordShadow(shadowRecords)
    }

    deps.log.debug({ count: records.length, total: emitted }, 'emitted records')
  }

  return { emit, emittedCount: () => emitted }
}

function recordChannel(parsed: Record<string, unknown>): string | undefined {
  const { channel } = parsed
  return typeof channel === 'string' && channel.length > 0 ? channel : undefined
}
