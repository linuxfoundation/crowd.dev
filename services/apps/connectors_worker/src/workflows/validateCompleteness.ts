import { log, proxyActivities } from '@temporalio/workflow'

import type { IValidationUnit } from '@crowd/data-access-layer/src/connectors'

import type * as activities from '../activities/completenessActivities'
import { describeChannelError } from './shadowDiff'

const { listValidationUnits } = proxyActivities<typeof activities>({
  startToCloseTimeout: '1 minute',
  retry: { maximumAttempts: 3, backoffCoefficient: 2 },
})

const { validateUnit } = proxyActivities<typeof activities>({
  startToCloseTimeout: '6 hours',
  heartbeatTimeout: '2 minutes',
  retry: { maximumAttempts: 3, backoffCoefficient: 2 },
})

export const VALIDATION_PAGE_SIZE = 10

async function runUnit(
  unit: IValidationUnit,
  input: activities.IValidationInput,
): Promise<activities.IUnitValidationResult> {
  try {
    return await validateUnit({ unit, window: input.window, counterKeys: input.counterKeys })
  } catch (err) {
    log.error('completeness validation activity failed for unit', { unitId: unit.id, err })
    return {
      unitId: unit.id,
      written: 0,
      failures: [{ unitId: unit.id, counterKey: '*', errorMessage: describeChannelError(err) }],
      noCounters: false,
    }
  }
}

export async function validateCompleteness(input: activities.IValidationInput): Promise<{
  unitCount: number
  resultCount: number
  failureCount: number
  noCounterUnitCount: number
}> {
  let afterId: string | undefined
  let unitCount = 0
  let resultCount = 0
  let failureCount = 0
  let noCounterUnitCount = 0

  for (;;) {
    const units = await listValidationUnits({
      target: input.target,
      afterId,
      limit: VALIDATION_PAGE_SIZE,
    })
    if (units.length === 0) {
      break
    }

    const results = await Promise.all(units.map((unit) => runUnit(unit, input)))
    for (const result of results) {
      resultCount += result.written
      failureCount += result.failures.length
      if (result.noCounters) {
        noCounterUnitCount++
      }
      for (const failure of result.failures) {
        log.error('completeness validation counter failed', { ...failure })
      }
    }

    unitCount += units.length
    afterId = units[units.length - 1].id
  }

  const summary = { unitCount, resultCount, failureCount, noCounterUnitCount }
  log.info('completeness_validation_summary', summary)
  return summary
}
