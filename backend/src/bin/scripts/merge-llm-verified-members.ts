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
  verdictCreatedAt: Date
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
    name: 'parallelism',
    alias: 'p',
    type: Number,
    description: 'Number of member merges to run at once. Defaults to 1.',
  },
  {
    name: 'agentUserId',
    alias: 'a',
    type: String,
    description: 'LF Agent user id to use as the audit log actor.',
  },
  {
    name: 'help',
    alias: 'h',
    type: Boolean,
    description: 'Print this usage guide.',
  },
]

const parameters = commandLineArgs(options)

function selectIndependentCandidates(
  candidates: LlmVerifiedMemberMergeCandidate[],
): LlmVerifiedMemberMergeCandidate[] {
  const selectedMemberIds = new Set<string>()
  const selected: LlmVerifiedMemberMergeCandidate[] = []

  for (const candidate of candidates) {
    const isConflicting =
      selectedMemberIds.has(candidate.primaryId) ||
      selectedMemberIds.has(candidate.secondaryId)

    if (isConflicting) {
      log.info(
        {
          primaryId: candidate.primaryId,
          secondaryId: candidate.secondaryId,
        },
        'Skipping LLM verified member merge because one member is already selected in this batch!',
      )
    } else {
      selectedMemberIds.add(candidate.primaryId)
      selectedMemberIds.add(candidate.secondaryId)
      selected.push(candidate)
    }
  }

  return selected
}

async function fetchLlmVerifiedMemberMergeCandidates(
  qx: QueryExecutor,
  batchSize: number,
  excludedMemberIds: string[],
): Promise<LlmVerifiedMemberMergeCandidate[]> {
  const excludedMemberFilter =
    excludedMemberIds.length > 0
      ? `
        AND l."primaryId" NOT IN ($(excludedMemberIds:csv))
        AND l."secondaryId" NOT IN ($(excludedMemberIds:csv))
      `
      : ''

  return qx.select(
    `
    SELECT
      l."primaryId",
      l."secondaryId",
      l."createdAt" AS "verdictCreatedAt"
    FROM "llmSuggestionVerdicts" l
    WHERE l.type = 'member'
      AND l.verdict = 'true'
      AND EXISTS (SELECT 1 FROM members m WHERE m.id = l."primaryId")
      AND EXISTS (SELECT 1 FROM members m WHERE m.id = l."secondaryId")
      AND NOT EXISTS (
        SELECT 1
        FROM "mergeActions" ma
        WHERE ma.type = 'member'
          AND (
            (ma."primaryId" = l."primaryId" AND ma."secondaryId" = l."secondaryId")
            OR (ma."primaryId" = l."secondaryId" AND ma."secondaryId" = l."primaryId")
          )
      )
      ${excludedMemberFilter}
    ORDER BY l."createdAt" ASC
    LIMIT $(batchSize);
    `,
    {
      batchSize,
      excludedMemberIds,
    },
  )
}

setImmediate(async () => {
  const testRun = parameters.testRun ?? false
  const BATCH_SIZE = parameters.batchSize ?? (testRun ? 10 : 100)
  const PARALLEL_MERGES = parameters.parallelism ?? 1
  const agentUserId = parameters.agentUserId ?? null

  if (!agentUserId) {
    log.error('LF Agent user id is required. Pass --agentUserId=<user-id>.')
    process.exit(1)
  }

  const options = await SequelizeRepository.getDefaultIRepositoryOptions({ id: agentUserId })
  const qx = SequelizeRepository.getQueryExecutor(options)
  const service = new CommonMemberService(optionsQx(options), options.temporal, log)
  const processedMemberIds = new Set<string>()

  log.info(
    { testRun, BATCH_SIZE, PARALLEL_MERGES, agentUserId },
    'Running script with the following parameters!',
  )

  let candidates: LlmVerifiedMemberMergeCandidate[] = []

  do {
    candidates = await fetchLlmVerifiedMemberMergeCandidates(
      qx,
      BATCH_SIZE,
      Array.from(processedMemberIds),
    )

    log.info({ count: candidates.length }, 'Fetched LLM verified member merge candidates!')

    const independentCandidates = selectIndependentCandidates(candidates)

    log.info(
      {
        count: independentCandidates.length,
        skipped: candidates.length - independentCandidates.length,
      },
      'Selected independent LLM verified member merge candidates!',
    )

    const chunks = chunkArray(independentCandidates, PARALLEL_MERGES)

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
            processedMemberIds.add(candidate.primaryId)
            processedMemberIds.add(candidate.secondaryId)
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
