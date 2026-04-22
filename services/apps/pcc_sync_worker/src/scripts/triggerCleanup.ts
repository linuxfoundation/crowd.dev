import { TEMPORAL_CONFIG, getTemporalClient } from '../config/settings'

async function main() {
  const client = await getTemporalClient(TEMPORAL_CONFIG())

  const workflowId = `pcc-cleanup/manual/${new Date().toISOString().slice(0, 19)}`

  await client.workflow.start('pccS3CleanupScheduler', {
    taskQueue: 'pccSync',
    workflowId,
    retry: {
      initialInterval: '15 seconds',
      backoffCoefficient: 2,
      maximumAttempts: 3,
    },
    args: [],
  })

  console.log(`PCC S3 cleanup workflow started: ${workflowId}`)
  process.exit(0)
}

main().catch((err) => {
  console.error('Failed to trigger workflow:', err)
  process.exit(1)
})
