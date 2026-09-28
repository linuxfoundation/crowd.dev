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

export interface ILlmSuggestionVerdictPair {
  id: string
  primaryId: string
  secondaryId: string
}

export async function findLlmApprovedMembersMarkedNoMerge(
  qx: QueryExecutor,
  { afterId, limit }: { afterId: string; limit: number },
): Promise<ILlmSuggestionVerdictPair[]> {
  return qx.select(
    `
      SELECT v.id, v."primaryId", v."secondaryId"
      FROM "llmSuggestionVerdicts" v
      WHERE v.type = 'member'
        AND v.id > $(afterId)
        AND (v.response ->> 'decision')::boolean
        AND EXISTS (
          SELECT 1
          FROM "memberNoMerge" nm
          WHERE (
              (nm."memberId" = v."primaryId" AND nm."noMergeId" = v."secondaryId")
              OR (nm."memberId" = v."secondaryId" AND nm."noMergeId" = v."primaryId")
            )
            -- only no-merge rows written by the LLM job, not ones added by people
            AND nm."createdAt" BETWEEN v."createdAt" - INTERVAL '10 minutes'
              AND v."createdAt" + INTERVAL '10 minutes'
        )
        AND NOT EXISTS (
          SELECT 1
          FROM "mergeActions" ma
          WHERE ma.type = 'member'
            AND (
              (ma."primaryId" = v."primaryId" AND ma."secondaryId" = v."secondaryId")
              OR (ma."primaryId" = v."secondaryId" AND ma."secondaryId" = v."primaryId")
            )
        )
        AND EXISTS (SELECT 1 FROM members m WHERE m.id = v."primaryId" AND m."deletedAt" IS NULL)
        AND EXISTS (SELECT 1 FROM members m WHERE m.id = v."secondaryId" AND m."deletedAt" IS NULL)
      ORDER BY v.id
      LIMIT $(limit)
    `,
    { afterId, limit },
  )
}
