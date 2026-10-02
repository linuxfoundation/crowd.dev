import { getErrorMessage } from '@crowd/common'
import { LlmService } from '@crowd/common_services'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceLogger } from '@crowd/logging'
import { createPccCandidatesLookup, IPccCandidateRow } from '@crowd/project-onboarding'
import { SnowflakeClient } from '@crowd/snowflake'
import { LlmQueryType } from '@crowd/types'

import { createCdpSegmentLookup } from './cdpSegmentLookup'
import { IRequestClassificationDeps } from './requestClassification'

const log = getServiceLogger()

function createQueryLlm(qx: QueryExecutor): IRequestClassificationDeps['queryLlm'] {
  const llmService = new LlmService(
    qx,
    {
      accessKeyId: process.env['CROWD_AWS_BEDROCK_ACCESS_KEY_ID'],
      secretAccessKey: process.env['CROWD_AWS_BEDROCK_SECRET_ACCESS_KEY'],
    },
    log,
  )

  return async (prompt) => {
    const response = await llmService.queryLlm(
      LlmQueryType.ONBOARDING_REQUEST_PARSING,
      prompt,
      undefined,
      undefined,
      false,
    )
    return response?.answer
  }
}

function createSnowflakeClient(): SnowflakeClient | null {
  if (!process.env.CROWD_SNOWFLAKE_ACCOUNT) {
    log.warn('Snowflake is not configured, PCC lookups are unavailable.')
    return null
  }

  try {
    return SnowflakeClient.fromEnv({ parentLog: log })
  } catch (err) {
    log.warn({ error: getErrorMessage(err) }, 'Snowflake client could not be created.')
    return null
  }
}

export async function withRequestClassificationDeps<T>(
  qx: QueryExecutor,
  run: (deps: IRequestClassificationDeps) => Promise<T>,
): Promise<T> {
  const snowflake = createSnowflakeClient()

  try {
    return await run({
      queryLlm: createQueryLlm(qx),
      lookups: {
        findPccCandidates: snowflake
          ? createPccCandidatesLookup((query, binds) =>
              snowflake.run<IPccCandidateRow>(query, binds),
            )
          : undefined,
        findCdpSegmentByPccProject: createCdpSegmentLookup(qx),
      },
    })
  } finally {
    await snowflake?.destroy()
  }
}
