import { TEMPORAL_CONFIG, getTemporalClient } from '@crowd/temporal'

import { osvSync } from '../osv/workflows'

async function main(): Promise<void> {
  const raw = process.argv[2]
  const ecosystems = raw ? raw.split(',').map((e) => e.trim()) : ['cargo']

  const cfg = TEMPORAL_CONFIG()
  if (!cfg.serverUrl || !cfg.namespace) {
    console.error('Missing CROWD_TEMPORAL_SERVER_URL or CROWD_TEMPORAL_NAMESPACE')
    process.exit(1)
  }

  const client = await getTemporalClient(cfg)

  const workflowId = `osv-sync-manual-${ecosystems.join('-')}-${Date.now()}`
  const handle = await client.workflow.start(osvSync, {
    taskQueue: 'osv-worker',
    workflowId,
    args: [{ ecosystems }],
  })

  console.log(`Started workflow ${handle.workflowId} (ecosystems: ${ecosystems.join(', ')})`)
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
