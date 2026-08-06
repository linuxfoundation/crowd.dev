import { TEMPORAL_CONFIG, getTemporalClient } from '@crowd/temporal'

import { ingestNuGetPackages } from '../nuget/workflows'

async function main(): Promise<void> {
  const cfg = TEMPORAL_CONFIG()
  if (!cfg.serverUrl || !cfg.namespace) {
    console.error('Missing CROWD_TEMPORAL_SERVER_URL or CROWD_TEMPORAL_NAMESPACE')
    process.exit(1)
  }

  const client = await getTemporalClient(cfg)

  const workflowId = `nuget-sync-manual-${Date.now()}`
  const handle = await client.workflow.start(ingestNuGetPackages, {
    taskQueue: 'nuget-worker',
    workflowId,
    args: [],
  })

  console.log(`Started workflow ${handle.workflowId}`)
  console.log('Waiting for completion...')

  await handle.result()
  console.log('Done')
}

main()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error('Failed:', err)
    process.exit(1)
  })
