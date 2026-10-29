import { randomUUID } from 'node:crypto'

import {
  assertKnownCounterKeys,
  getManifest,
  registerConnector,
  selectCounters,
} from '@crowd/connectors'
import { credlyConnector } from '@crowd/connectors/src/connectors/credly'
import { githubConnector } from '@crowd/connectors/src/connectors/github'
import { octolensConnector } from '@crowd/connectors/src/connectors/octolens'
import {
  listValidationUnitsPage,
  setUnitsValidation,
} from '@crowd/data-access-layer/src/connectors'
import type { IValidationUnit } from '@crowd/data-access-layer/src/connectors/types'
import { WRITE_DB_CONFIG, getDbConnection } from '@crowd/data-access-layer/src/database'
import { QueryExecutor, pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceLogger } from '@crowd/logging'
import { TEMPORAL_CONFIG, getTemporalClient } from '@crowd/temporal'

import type { IValidationInput } from '../activities/completenessActivities'
import {
  IValidateCompletenessArgs,
  nextCounterKeys,
  parseValidateCompletenessArgs,
} from './validateCompletenessArgs'

const log = getServiceLogger()

const UNITS_PAGE_SIZE = 500

registerConnector(githubConnector)
registerConnector(octolensConnector)
registerConnector(credlyConnector)

function usage(reason: string): never {
  log.error(
    `${reason}\n` +
      'Usage:\n' +
      '  validate-completeness run <target> [--from <iso> --to <iso> | --all-time] [--counters k1,k2]\n' +
      '  validate-completeness enable <target> [--counters k1,k2]\n' +
      '  validate-completeness disable <target> [--counters k1,k2]\n' +
      '  <target> = --unit-id <id>[,<id>] | --repo <owner/repo|url> | --integration-id <id>',
  )
  process.exit(1)
}

async function listTargetUnits(
  qx: QueryExecutor,
  target: IValidateCompletenessArgs['target'],
): Promise<IValidationUnit[]> {
  const units: IValidationUnit[] = []
  for (;;) {
    const page = await listValidationUnitsPage(qx, target, {
      afterId: units[units.length - 1]?.id,
      limit: UNITS_PAGE_SIZE,
      includeDisabled: true,
    })
    if (page.length === 0) {
      return units
    }
    units.push(...page)
  }
}

function assertCounterKeys(units: IValidationUnit[], counterKeys: string[]): void {
  for (const platform of new Set(units.map((unit) => unit.platform))) {
    assertKnownCounterKeys(getManifest(platform), counterKeys)
  }
}

function defaultCounterKeys(unit: IValidationUnit): string[] {
  return selectCounters(getManifest(unit.platform), unit.syncName, 'range', null).map(
    (counter) => counter.key,
  )
}

function syncCounterKeys(unit: IValidationUnit): string[] {
  return (getManifest(unit.platform).counters ?? [])
    .filter((counter) => counter.syncName === unit.syncName)
    .map((counter) => counter.key)
}

async function updateCounterKeys(
  qx: QueryExecutor,
  units: IValidationUnit[],
  counterKeys: string[],
  command: 'enable' | 'disable',
): Promise<number> {
  const idsByKeys = new Map<string, string[]>()
  for (const unit of units) {
    if (command === 'disable' && !unit.validationEnabled) {
      continue
    }
    const unitKeys = syncCounterKeys(unit).filter((key) => counterKeys.includes(key))
    if (unitKeys.length === 0) {
      continue
    }
    const keys = nextCounterKeys(
      unit.validationCounterKeys ?? defaultCounterKeys(unit),
      unitKeys,
      command,
    )
    const ids = idsByKeys.get(JSON.stringify(keys)) ?? []
    ids.push(unit.id)
    idsByKeys.set(JSON.stringify(keys), ids)
  }

  let updated = 0
  for (const [serialized, ids] of idsByKeys) {
    const keys: string[] = JSON.parse(serialized)
    updated +=
      keys.length === 0
        ? await setUnitsValidation(qx, ids, false, null)
        : await setUnitsValidation(qx, ids, true, keys)
  }
  return updated
}

async function runValidation(input: IValidationInput): Promise<boolean> {
  const temporal = await getTemporalClient(TEMPORAL_CONFIG())
  const workflowId = `validate-completeness-${randomUUID()}`

  log.info({ workflowId, ...input }, 'starting completeness validation')
  const handle = await temporal.workflow.start('validateCompleteness', {
    taskQueue: 'connectors',
    workflowId,
    args: [input],
  })

  const summary = await handle.result()
  console.table([summary])
  return summary.failureCount === 0
}

setImmediate(async () => {
  let args: IValidateCompletenessArgs
  try {
    args = parseValidateCompletenessArgs(process.argv.slice(2))
  } catch (err) {
    usage(err instanceof Error ? err.message : String(err))
  }

  try {
    const { command, target, window, counterKeys } = args
    const qx = pgpQx(await getDbConnection(WRITE_DB_CONFIG()))

    if (command === 'run') {
      if (counterKeys) {
        assertCounterKeys(await listTargetUnits(qx, target), counterKeys)
      }
      const succeeded = await runValidation({
        target,
        window: window as NonNullable<typeof window>,
        counterKeys,
      })
      process.exit(succeeded ? 0 : 1)
    }

    const units = await listTargetUnits(qx, target)
    if (counterKeys) {
      assertCounterKeys(units, counterKeys)
    }
    const updated = counterKeys
      ? await updateCounterKeys(qx, units, counterKeys, command)
      : await setUnitsValidation(
          qx,
          units.map((unit) => unit.id),
          command === 'enable',
          null,
        )
    log.info({ command, resolved: units.length, updated }, 'validation flag updated')
    process.exit(0)
  } catch (err) {
    log.error(err, 'completeness validation command failed')
    process.exit(1)
  }
})
