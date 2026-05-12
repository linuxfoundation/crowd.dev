import commandLineArgs from 'command-line-args'

import { CommonMemberService } from '@crowd/common_services'
import { QueryExecutor, optionsQx } from '@crowd/data-access-layer'
import { chunkArray } from '@crowd/data-access-layer/src/old/apps/merge_suggestions_worker/utils'
import { getServiceLogger } from '@crowd/logging'

import SequelizeRepository from '../../database/repositories/sequelizeRepository'

const log = getServiceLogger()

interface LlmVerifiedMemberMergeCandidate {
  primaryId: string
  secondaryId: string
  mergeState: string | null
  mergeStep: string | null
  verdictCreatedAt: Date
  suggestionCreatedAt: Date
}

const options = [
  {
    name: 'testRun',
    alias: 't',
    type: Boolean,
    description: 'Run in test mode (limit to 10 member merge candidates).',
  },
  {
    name: 'batchSize',
    alias: 'b',
    type: Number,
    description: 'Number of candidates to fetch per batch.',
  },
  {
    name: 'help',
    alias: 'h',
    type: Boolean,
    description: 'Print this usage guide.',
  },
]

const parameters = commandLineArgs(options)

async function fetchLlmVerifiedMemberMergeCandidates(
  qx: QueryExecutor,
  batchSize: number,
): Promise<LlmVerifiedMemberMergeCandidate[]> {
  return qx.select(
    `
    WITH stale_true_suggestions AS (
      SELECT
        s."memberId",
        s."toMergeId",
        s.similarity,
        s."createdAt" AS "suggestionCreatedAt"
      FROM "memberToMerge" s
      WHERE s.similarity > 0.8
        AND s."createdAt" < NOW() - INTERVAL '7 days'
    ),
    latest_true_verdict AS (
      SELECT DISTINCT ON (s."memberId", s."toMergeId")
        s."memberId",
        s."toMergeId",
        l."primaryId",
        l."secondaryId",
        l."createdAt" AS "verdictCreatedAt"
      FROM stale_true_suggestions s
      JOIN "llmSuggestionVerdicts" l
        ON l.type = 'member'
       AND l.verdict = 'true'
       AND (
            (l."primaryId" = s."memberId" AND l."secondaryId" = s."toMergeId")
         OR (l."primaryId" = s."toMergeId" AND l."secondaryId" = s."memberId")
       )
      ORDER BY s."memberId", s."toMergeId", l."createdAt" DESC
    ),
    latest_merge_action AS (
      SELECT DISTINCT ON (v."memberId", v."toMergeId")
        v."memberId",
        v."toMergeId",
        ma.state,
        ma.step,
        ma."createdAt"
      FROM latest_true_verdict v
      LEFT JOIN "mergeActions" ma
        ON ma.type = 'member'
       AND (
            (ma."primaryId" = v."primaryId" AND ma."secondaryId" = v."secondaryId")
         OR (ma."primaryId" = v."secondaryId" AND ma."secondaryId" = v."primaryId")
       )
      ORDER BY v."memberId", v."toMergeId", ma."createdAt" DESC NULLS LAST
    )
    SELECT
      v."primaryId",
      v."secondaryId",
      ma.state AS "mergeState",
      ma.step AS "mergeStep",
      v."verdictCreatedAt",
      s."suggestionCreatedAt"
    FROM latest_true_verdict v
    JOIN stale_true_suggestions s
      ON s."memberId" = v."memberId"
     AND s."toMergeId" = v."toMergeId"
    LEFT JOIN latest_merge_action ma
      ON ma."memberId" = v."memberId"
     AND ma."toMergeId" = v."toMergeId"
    WHERE (ma.state = 'error' OR ma.state IS NULL)
      AND EXISTS (SELECT 1 FROM members m WHERE m.id = v."primaryId")
      AND EXISTS (SELECT 1 FROM members m WHERE m.id = v."secondaryId")
      AND NOT EXISTS (
        SELECT 1
        FROM "memberNoMerge" n
        WHERE (n."memberId" = v."primaryId" AND n."noMergeId" = v."secondaryId")
           OR (n."memberId" = v."secondaryId" AND n."noMergeId" = v."primaryId")
      )
    ORDER BY v."verdictCreatedAt" ASC
    LIMIT $(batchSize);
    `,
    {
      batchSize,
    },
  )
}

setImmediate(async () => {
  const testRun = parameters.testRun ?? false
  const BATCH_SIZE = parameters.batchSize ?? (testRun ? 10 : 100)
  const PARALLEL_MERGES = 10

  const options = await SequelizeRepository.getDefaultIRepositoryOptions()
  const qx = SequelizeRepository.getQueryExecutor(options)
  const service = new CommonMemberService(optionsQx(options), options.temporal, log)

  log.info({ testRun, BATCH_SIZE }, 'Running script with the following parameters!')

  let candidates: LlmVerifiedMemberMergeCandidate[] = []

  do {
    candidates = await fetchLlmVerifiedMemberMergeCandidates(qx, BATCH_SIZE)

    log.info({ count: candidates.length }, 'Fetched LLM verified member merge candidates!')

    const chunks = chunkArray(candidates, PARALLEL_MERGES)

    for (const chunk of chunks) {
      await Promise.all(
        chunk.map(async (candidate) => {
          log.info(
            {
              primaryId: candidate.primaryId,
              secondaryId: candidate.secondaryId,
            },
            'Retrying LLM verified member merge!',
          )

          try {
            await service.merge(candidate.primaryId, candidate.secondaryId)
          } catch (err) {
            log.error(
              {
                primaryId: candidate.primaryId,
                secondaryId: candidate.secondaryId,
                err,
              },
              'Failed retrying LLM verified member merge!',
            )
            throw err
          }
        }),
      )
    }

    if (testRun) {
      log.info('Test run - stopping after first batch!')
      break
    }
  } while (candidates.length > 0)

  process.exit(0)
})
