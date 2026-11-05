import { getServiceLogger } from '@crowd/logging'
import { TEMPORAL_CONFIG, getTemporalClient } from '@crowd/temporal'

import type { ISwitchSyncResult, SwitchDirection } from '../syncPipelineSwitch'
import { switchSyncPipelineWorkflowId } from '../workflows/switchSyncPipeline'
import { parseRepoArg } from './repoArg'

const log = getServiceLogger()

const DIRECTIONS: SwitchDirection[] = ['connectors', 'nango']

function usage(): never {
  log.error(
    'Usage: switch-sync-pipeline <owner/repo | github url> <connectors|nango> [sync1,sync2,...]\n' +
      "  'connectors' pauses the nango syncs and makes the connector units emit; 'nango' reverses it.\n" +
      '  Switch a repo back to nango before re-mirroring it after a rename, or the drop-on-read guard stops matching.',
  )
  process.exit(1)
}

function parseArgs(rawArgv: string[]): {
  owner: string
  repo: string
  direction: SwitchDirection
  syncNames?: string[]
} {
  const argv = rawArgv.filter((arg) => arg !== '--')
  if (argv.length < 2 || argv.length > 3) {
    usage()
  }
  const [repoArg, direction, syncsArg] = argv
  if (!DIRECTIONS.includes(direction as SwitchDirection)) {
    usage()
  }
  const { owner, name } = parseRepoArg(repoArg)
  const syncNames = syncsArg
    ? syncsArg
        .split(',')
        .map((s) => s.trim())
        .filter(Boolean)
    : undefined
  return { owner, repo: name, direction: direction as SwitchDirection, syncNames }
}

setImmediate(async () => {
  try {
    const input = parseArgs(process.argv.slice(2))
    const temporal = await getTemporalClient(TEMPORAL_CONFIG())
    const workflowId = switchSyncPipelineWorkflowId(input.owner, input.repo)

    log.info({ workflowId, ...input }, 'starting sync pipeline switch')
    const handle = await temporal.workflow.start('switchSyncPipeline', {
      taskQueue: 'connectors',
      workflowId,
      args: [input],
    })

    const results: ISwitchSyncResult[] = await handle.result()
    console.table(results)

    const refused = results.filter((r) => r.outcome === 'refused')
    log.info(
      { switched: results.length - refused.length, refused: refused.length },
      'sync pipeline switch finished',
    )
    process.exit(refused.length > 0 ? 1 : 0)
  } catch (err) {
    if (err instanceof Error && err.name === 'WorkflowExecutionAlreadyStartedError') {
      log.error('a switch for this repo is already running - wait for it to finish')
      process.exit(1)
    }
    log.error(err, 'sync pipeline switch failed')
    process.exit(1)
  }
})
