import { ILLMSuggestionVerdict } from '@crowd/types'

import { QueryExecutor } from '../queryExecutor'

export async function insertLlmSuggestionVerdict(
  qx: QueryExecutor,
  verdict: ILLMSuggestionVerdict,
): Promise<void> {
  await qx.result(
    `
      INSERT INTO "llmSuggestionVerdicts" (
        "type",
        "model",
        "primaryId",
        "secondaryId",
        "prompt",
        "response",
        "inputTokenCount",
        "outputTokenCount",
        "responseTimeSeconds"
      )
      VALUES (
        $(type),
        $(model),
        $(primaryId),
        $(secondaryId),
        $(prompt),
        $(response),
        $(inputTokenCount),
        $(outputTokenCount),
        $(responseTimeSeconds)
      )
    `,
    {
      type: verdict.type,
      model: verdict.model,
      primaryId: verdict.primaryId,
      secondaryId: verdict.secondaryId,
      prompt: verdict.prompt,
      response: JSON.stringify(verdict.response),
      inputTokenCount: verdict.inputTokenCount,
      outputTokenCount: verdict.outputTokenCount,
      responseTimeSeconds: verdict.responseTimeSeconds,
    },
  )
}
