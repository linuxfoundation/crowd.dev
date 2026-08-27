import { ZodError, ZodType } from 'zod'

import type { DataSinkWorkerEmitter } from '@crowd/common_services'
import type { ISyncUnit } from '@crowd/data-access-layer/src/connectors'
import type { Logger } from '@crowd/logging'
import { IIntegrationResult, IntegrationResultType } from '@crowd/types'

import { ConnectorError } from './http/errors'

export interface EmitterDeps {
  publishResult: (integrationId: string, result: IIntegrationResult) => Promise<string>
  sinkEmitter: DataSinkWorkerEmitter
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

      const resultId = await deps.publishResult(deps.unit.integrationId, {
        type: IntegrationResultType.ACTIVITY,
        segmentId: deps.segmentId,
        data: payload,
      })
      await deps.sinkEmitter.triggerResultProcessing(resultId, resultId, false)

      emitted += 1
    }

    deps.log.debug({ count: records.length, total: emitted }, 'emitted records')
  }

  return { emit, emittedCount: () => emitted }
}
