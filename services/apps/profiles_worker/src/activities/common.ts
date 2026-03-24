import { LlmService } from '@crowd/common_services'
import { pgpQx } from '@crowd/data-access-layer'
import { ILlmResponse, LlmQueryType } from '@crowd/types'

import { svc } from '../main'

export async function getLLMResult(
  queryType: LlmQueryType,
  prompt: string,
  entityId: string,
  saveHistory = true,
  metadata?: Record<string, string>,
): Promise<ILlmResponse> {
  const qx = pgpQx(svc.postgres.writer.connection())
  const llmService = new LlmService(
    qx,
    {
      accessKeyId: process.env['CROWD_AWS_BEDROCK_ACCESS_KEY_ID'],
      secretAccessKey: process.env['CROWD_AWS_BEDROCK_SECRET_ACCESS_KEY'],
    },
    svc.log,
  )

  return llmService.queryLlm(queryType, prompt, entityId, metadata, saveHistory)
}
