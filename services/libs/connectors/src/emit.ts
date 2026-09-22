import { ZodError, ZodType } from 'zod'

import type { DataSinkWorkerEmitter } from '@crowd/common_services'
import type { IShadowRecord, ISyncUnit } from '@crowd/data-access-layer/src/connectors'
import type { Logger } from '@crowd/logging'
import { IIntegrationResult, IntegrationResultType } from '@crowd/types'

import { ConnectorError } from './http/errors'

export interface EmitterDeps {
  publishResult: (integrationId: string, result: IIntegrationResult) => Promise<string>
  sinkEmitter: DataSinkWorkerEmitter
  recordShadow: (records: IShadowRecord[]) => Promise<void>
  unit: ISyncUnit
  segmentId: string
  schema: ZodType<Record<string, unknown>>
  log: Logger
}

export interface Emitter {
  emit: (records: unknown[]) => Promise<void>
  emittedCount: () => number
}

export function createEmit(deps: EmitterDeps): Emitter {
  let emitted = 0

  const emit = async (records: unknown[]): Promise<void> => {
    const shadowRecords: IShadowRecord[] = []

    for (const record of records) {
      let parsed: Record<string, unknown>
      try {
        parsed = deps.schema.parse(record)
      } catch (err) {
        if (err instanceof ZodError) {
          throw new ConnectorError('connector.code', 'record failed schema validation', {
            cause: err,
          })
        }
        throw err
      }

      const payload = { ...parsed, channel: deps.unit.channelName }

      if (!deps.unit.emitEnabled) {
        const { type, sourceId, timestamp } = payload as Record<string, unknown>
        if (typeof type !== 'string' || typeof sourceId !== 'string' || !timestamp) {
          throw new ConnectorError(
            'connector.code',
            'record missing type/sourceId/timestamp required for shadow mode',
          )
        }
        shadowRecords.push({
          type,
          sourceId,
          occurredAt: String(timestamp),
          data: payload,
        })
        emitted += 1
        continue
      }

      try {
        const resultId = await deps.publishResult(deps.unit.integrationId, {
          type: IntegrationResultType.ACTIVITY,
          segmentId: deps.segmentId,
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
