import commandLineArgs from 'command-line-args'

import { DEFAULT_TENANT_ID } from '@crowd/common'
import { fetchFakeOrganizationAnalysisCandidates, pgpQx } from '@crowd/data-access-layer'
import { getDbConnection } from '@crowd/data-access-layer/src/database'
import { getServiceLogger } from '@crowd/logging'
import { getTemporalClient } from '@crowd/temporal'
import { TemporalWorkflowId } from '@crowd/types'

import { DB_CONFIG, TEMPORAL_CONFIG } from '@/conf'

const log = getServiceLogger()

const options = [
  {
    name: 'testRun',
    alias: 't',
    type: Boolean,
    description: 'Run in test mode (limit to 10 organizations).',
  },
  {
    name: 'afterOrganizationId',
    alias: 'a',
    type: String,
    description: 'The organization ID to start processing after.',
  },
  {
    name: 'help',
    alias: 'h',
    type: Boolean,
    description: 'Print this usage guide.',
  },
]

const parameters = commandLineArgs(options)

setImmediate(async () => {
  const testRun = parameters.testRun ?? false
  const BATCH_SIZE = testRun ? 10 : 100
  let afterOrganizationId = parameters.afterOrganizationId ?? undefined

  const db = await getDbConnection({
    host: DB_CONFIG.readHost,
    port: DB_CONFIG.port,
    database: DB_CONFIG.database,
    user: DB_CONFIG.username,
    password: DB_CONFIG.password,
  })

  const qx = pgpQx(db)
  const temporal = await getTemporalClient(TEMPORAL_CONFIG)

  log.info(
    { testRun, BATCH_SIZE, afterOrganizationId },
    'Running script with the following parameters!',
  )

  let organizationIds: string[] = []

  do {
    organizationIds = await fetchFakeOrganizationAnalysisCandidates(
      qx,
      BATCH_SIZE,
      afterOrganizationId,
    )

    for (const organizationId of organizationIds) {
      log.info({ organizationId }, 'Triggering workflow for organization!')

      const workflowId = `${TemporalWorkflowId.FAKE_ORGANIZATION_ANALYSIS_WITH_LLM}/${organizationId}`

      try {
        await temporal.workflow.start('fakeOrganizationAnalysisWithLLM', {
          taskQueue: 'profiles',
          workflowId,
          retry: {
            maximumAttempts: 10,
          },
          args: [{ organizationId }],
          searchAttributes: {
            TenantId: [DEFAULT_TENANT_ID],
          },
        })

        await temporal.workflow.result(workflowId)
      } catch (err) {
        log.error({ organizationId, err }, 'Failed to trigger workflow for organization!')
        throw err
      }
    }

    if (organizationIds.length > 0) {
      afterOrganizationId = organizationIds[organizationIds.length - 1]
      log.info(
        { afterOrganizationId, count: organizationIds.length },
        'Batch processed!',
      )
    }

    if (testRun) {
      log.info('Test run - stopping after first batch!')
      break
    }
  } while (organizationIds.length > 0)

  process.exit(0)
})
