import commandLineArgs from 'command-line-args'

import { DB_CONFIG, TEMPORAL_CONFIG } from '@/conf'
import { DEFAULT_TENANT_ID } from '@crowd/common'
import { fetchBotCandidateMembers, pgpQx } from '@crowd/data-access-layer'
import { getDbConnection } from '@crowd/data-access-layer/src/database'
import { chunkArray } from '@crowd/data-access-layer/src/old/apps/merge_suggestions_worker/utils'
import { getServiceLogger } from '@crowd/logging'
import { getTemporalClient } from '@crowd/temporal'

const log = getServiceLogger()

const options = [
  {
    name: 'testRun',
    alias: 't',
    type: Boolean,
    description: 'Run in test mode (limit to 10 members).',
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

  const db = await getDbConnection({
    host: DB_CONFIG.readHost,
    port: DB_CONFIG.port,
    database: DB_CONFIG.database,
    user: DB_CONFIG.username,
    password: DB_CONFIG.password,
  })

  const qx = pgpQx(db)
  const temporal = await getTemporalClient(TEMPORAL_CONFIG)

  log.info({ testRun, BATCH_SIZE }, 'Running script with the following parameters!')

  let botLikeMembers = []

  do {
    botLikeMembers = await fetchBotCandidateMembers(qx, BATCH_SIZE)

    const chunks = chunkArray(botLikeMembers, 10)

    for (const chunk of chunks) {
      // parallel processing
      await Promise.all(
        chunk.map(async (memberId) => {
          if (testRun) {
            log.info({ memberId }, 'Triggering workflow for member!')
          }

          try {
            await temporal.workflow.start('processMemberBotAnalysisWithLLM', {
              taskQueue: 'profiles',
              workflowId: `member-bot-analysis-with-llm/${memberId}`,
              retry: {
                maximumAttempts: 10,
              },
              args: [{ memberId }],
              searchAttributes: {
                TenantId: [DEFAULT_TENANT_ID],
              },
            })

            // wait till the workflow is finished
            await temporal.workflow.result(`member-bot-analysis-with-llm/${memberId}`)
          } catch (err) {
            log.error({ memberId, err }, 'Failed to trigger workflow for member!')
            throw err
          }
        }),
      )
    }

    if (testRun) {
      log.info('Test run - stopping after first batch!')
      break
    }
  } while (botLikeMembers.length > 0)

  process.exit(0)
})
