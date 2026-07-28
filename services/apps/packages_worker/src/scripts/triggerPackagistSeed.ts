import { TEMPORAL_CONFIG, getTemporalClient } from '@crowd/temporal'

import {
  ingestPackagistDownloads30d,
  ingestPackagistDownloadsDaily,
  ingestPackagistMetadata,
  seedPackagistPackages,
} from '../packagist/workflows'

const HELP = `
Usage: trigger-packagist [seed|metadata|downloads-30d|downloads-daily]

Arguments:
  seed             Fetch packagist.org/packages/list.json and seed the packages table (default)
  metadata         Crawl the dynamic (package info) + p2 (versions/dependencies) endpoints
  downloads-30d    Capture the observed rolling 30d window for every package
  downloads-daily  Capture daily downloads for the critical slice

Examples:
  pnpm trigger-packagist:local
  pnpm trigger-packagist:local seed
  pnpm trigger-packagist:local metadata
  pnpm trigger-packagist:local downloads-30d
  pnpm trigger-packagist:local downloads-daily
`

const TARGETS = ['seed', 'metadata', 'downloads-30d', 'downloads-daily'] as const
type Target = (typeof TARGETS)[number]

async function main(): Promise<void> {
  const args = process.argv.slice(2)
  if (args.includes('--help') || args.includes('-h')) {
    console.log(HELP)
    process.exit(0)
  }

  const target = (args[0] ?? 'seed') as Target
  if (!TARGETS.includes(target)) {
    console.error(`Unknown target "${target}". Use one of: ${TARGETS.join(', ')}.`)
    process.exit(1)
  }

  const cfg = TEMPORAL_CONFIG()
  if (!cfg.serverUrl || !cfg.namespace) {
    console.error('Missing CROWD_TEMPORAL_SERVER_URL or CROWD_TEMPORAL_NAMESPACE')
    process.exit(1)
  }

  const client = await getTemporalClient(cfg)
  const now = Date.now()

  if (target === 'seed') {
    const handle = await client.workflow.start(seedPackagistPackages, {
      taskQueue: 'packagist-worker',
      workflowId: `packagist-seed-manual-${now}`,
      args: [],
    })
    console.log(`Started workflow ${handle.workflowId}`)
    return
  }

  const workflow =
    target === 'metadata'
      ? ingestPackagistMetadata
      : target === 'downloads-30d'
        ? ingestPackagistDownloads30d
        : ingestPackagistDownloadsDaily

  const handle = await client.workflow.start(workflow, {
    taskQueue: 'packagist-worker',
    workflowId: `packagist-${target}-manual-${now}`,
    args: [{}],
  })
  console.log(`Started workflow ${handle.workflowId}`)
}

main()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error('Failed to trigger packagist workflow:', err)
    process.exit(1)
  })
