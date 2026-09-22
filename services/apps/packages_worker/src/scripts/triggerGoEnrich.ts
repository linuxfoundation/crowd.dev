import { TEMPORAL_CONFIG, getTemporalClient } from '@crowd/temporal'

import { enrichGoStatus, enrichGoVersions } from '../go/workflows'

const HELP = `
Usage: trigger-go [versions|status|both]

Arguments:
  versions   Enrich latest_version + latest_release_at from proxy.golang.org
  status     Enrich status from pkg.go.dev
  both       Trigger both (default)

Examples:
  pnpm trigger-go:local
  pnpm trigger-go:local versions
  pnpm trigger-go:local status
`

async function main(): Promise<void> {
  const args = process.argv.slice(2)
  if (args.includes('--help') || args.includes('-h')) {
    console.log(HELP)
    process.exit(0)
  }

  const target = (args[0] ?? 'both') as 'versions' | 'status' | 'both'
  if (!['versions', 'status', 'both'].includes(target)) {
    console.error(`Unknown target "${target}". Use "versions", "status", or "both".`)
    process.exit(1)
  }

  const cfg = TEMPORAL_CONFIG()
  if (!cfg.serverUrl || !cfg.namespace) {
    console.error('Missing CROWD_TEMPORAL_SERVER_URL or CROWD_TEMPORAL_NAMESPACE')
    process.exit(1)
  }

  const client = await getTemporalClient(cfg)
  const now = Date.now()

  if (target === 'versions' || target === 'both') {
    const handle = await client.workflow.start(enrichGoVersions, {
      taskQueue: 'go-worker',
      workflowId: `go-version-enrich-manual-${now}`,
      args: [],
    })
    console.log(`Started workflow ${handle.workflowId}`)
  }

  if (target === 'status' || target === 'both') {
    const handle = await client.workflow.start(enrichGoStatus, {
      taskQueue: 'go-worker',
      workflowId: `go-status-enrich-manual-${now}`,
      args: [],
    })
    console.log(`Started workflow ${handle.workflowId}`)
  }
}

main()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error('Failed to trigger go enrich:', err)
    process.exit(1)
  })
