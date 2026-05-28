// Usage: tsx src/scripts/triggerBootstrap.ts [full|incremental]
import { Client, Connection } from '@temporalio/client'

import { bootstrapOsspckgs } from '../deps-dev/workflows'

async function main(): Promise<void> {
  const mode = (process.argv[2] ?? 'full') as 'full' | 'incremental'
  if (mode !== 'full' && mode !== 'incremental') {
    console.error(`Unknown mode "${mode}". Use "full" or "incremental".`)
    process.exit(1)
  }

  const serverUrl = process.env.CROWD_TEMPORAL_SERVER_URL
  const namespace = process.env.CROWD_TEMPORAL_NAMESPACE
  if (!serverUrl || !namespace) {
    console.error('Missing CROWD_TEMPORAL_SERVER_URL or CROWD_TEMPORAL_NAMESPACE')
    process.exit(1)
  }

  const connection = await Connection.connect({ address: serverUrl })
  const client = new Client({ connection, namespace })

  const workflowId = `bootstrap-osspckgs-${mode}-${Date.now()}`
  const handle = await client.workflow.start(bootstrapOsspckgs, {
    taskQueue: 'deps-dev-ingest',
    workflowId,
    args: [{ mode }],
  })

  console.log(`Started workflow ${handle.workflowId}`)
  await connection.close()
}

main().catch((err) => {
  console.error('Failed to trigger bootstrap:', err)
  process.exit(1)
})
